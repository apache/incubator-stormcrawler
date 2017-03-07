/**
 * Licensed to DigitalPebble Ltd under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * DigitalPebble licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.digitalpebble.stormcrawler.bolt;

import java.io.File;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.metric.api.MeanReducer;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.metric.api.MultiReducedMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.Constants;
import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.persistence.Status;
import com.digitalpebble.stormcrawler.protocol.HttpHeaders;
import com.digitalpebble.stormcrawler.protocol.Protocol;
import com.digitalpebble.stormcrawler.protocol.ProtocolFactory;
import com.digitalpebble.stormcrawler.protocol.ProtocolResponse;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import com.digitalpebble.stormcrawler.util.PerSecondReducer;

import crawlercommons.robots.BaseRobotRules;
import crawlercommons.domains.PaidLevelDomain;

/**
 * A multithreaded, queue-based fetcher adapted from Apache Nutch. Enforces the
 * politeness and handles the fetching threads itself.
 */
@SuppressWarnings("serial")
public class FetcherBolt extends StatusEmitterBolt {

    private static final org.slf4j.Logger LOG = LoggerFactory
            .getLogger(FetcherBolt.class);

    private final AtomicInteger activeThreads = new AtomicInteger(0);
    private final AtomicInteger spinWaiting = new AtomicInteger(0);

    private FetchItemQueues fetchQueues;

    private MultiCountMetric eventCounter;
    private MultiReducedMetric averagedMetrics;

    private ProtocolFactory protocolFactory;

    private int taskID = -1;

    boolean sitemapsAutoDiscovery = false;

    private MultiReducedMetric perSecMetrics;

    private File debugfiletrigger;

    /** blocks the processing of new URLs if this value is reached **/
    private int maxNumberURLsInQueues = -1;

    /**
     * This class described the item to be fetched.
     */
    private static class FetchItem {

        String queueID;
        String url;
        URL u;
        Tuple t;

        public FetchItem(String url, URL u, Tuple t, String queueID) {
            this.url = url;
            this.u = u;
            this.queueID = queueID;
            this.t = t;
        }

        /**
         * Create an item. Queue id will be created based on
         * <code>queueMode</code> argument, either as a protocol + hostname
         * pair, protocol + IP address pair or protocol+domain pair.
         */

        public static FetchItem create(URL u, Tuple t, String queueMode) {

            String queueID;

            String url = u.toExternalForm();

            String key = null;
            // reuse any key that might have been given
            // be it the hostname, domain or IP
            if (t.contains("key")) {
                key = t.getStringByField("key");
            }
            if (StringUtils.isNotBlank(key)) {
                queueID = key.toLowerCase(Locale.ROOT);
                return new FetchItem(url, u, t, queueID);
            }

            if (FetchItemQueues.QUEUE_MODE_IP.equalsIgnoreCase(queueMode)) {
                try {
                    final InetAddress addr = InetAddress.getByName(u.getHost());
                    key = addr.getHostAddress();
                } catch (final UnknownHostException e) {
                    LOG.warn(
                            "Unable to resolve IP for {}, using hostname as key.",
                            u.getHost());
                    key = u.getHost();
                }
            } else if (FetchItemQueues.QUEUE_MODE_DOMAIN
                    .equalsIgnoreCase(queueMode)) {
                key = PaidLevelDomain.getPLD(u.getHost());
                if (key == null) {
                    LOG.warn(
                            "Unknown domain for url: {}, using hostname as key",
                            url);
                    key = u.getHost();
                }
            } else {
                key = u.getHost();
            }

            if (key == null) {
                LOG.warn("Unknown host for url: {}, using URL string as key",
                        url);
                key = u.toExternalForm();
            }

            queueID = key.toLowerCase(Locale.ROOT);
            return new FetchItem(url, u, t, queueID);
        }

    }

    /**
     * This class handles FetchItems which come from the same host ID (be it a
     * proto/hostname or proto/IP pair). It also keeps track of requests in
     * progress and elapsed time between requests.
     */
    private static class FetchItemQueue {
        Deque<FetchItem> queue = new LinkedBlockingDeque<>();

        AtomicInteger inProgress = new AtomicInteger();
        AtomicLong nextFetchTime = new AtomicLong();

        long crawlDelay;
        final long minCrawlDelay;
        final int maxThreads;

        public FetchItemQueue(Config conf, int maxThreads, long crawlDelay,
                long minCrawlDelay) {
            this.maxThreads = maxThreads;
            this.crawlDelay = crawlDelay;
            this.minCrawlDelay = minCrawlDelay;
            // ready to start
            setEndTime(System.currentTimeMillis() - crawlDelay);
        }

        public int getQueueSize() {
            return queue.size();
        }

        public int getInProgressSize() {
            return inProgress.get();
        }

        public void finishFetchItem(FetchItem it, boolean asap) {
            if (it != null) {
                inProgress.decrementAndGet();
                setEndTime(System.currentTimeMillis(), asap);
            }
        }

        public void addFetchItem(FetchItem it) {
            queue.add(it);
        }

        public FetchItem getFetchItem() {
            if (inProgress.get() >= maxThreads)
                return null;
            long now = System.currentTimeMillis();
            if (nextFetchTime.get() > now)
                return null;
            FetchItem it = null;
            if (queue.isEmpty())
                return null;
            try {
                it = queue.removeFirst();
                inProgress.incrementAndGet();
            } catch (Exception e) {
                LOG.error(
                        "Cannot remove FetchItem from queue or cannot add it to inProgress queue",
                        e);
            }
            return it;
        }

        private void setEndTime(long endTime) {
            setEndTime(endTime, false);
        }

        private void setEndTime(long endTime, boolean asap) {
            if (!asap)
                nextFetchTime.set(endTime
                        + (maxThreads > 1 ? minCrawlDelay : crawlDelay));
            else
                nextFetchTime.set(endTime);
        }

    }

    /**
     * Convenience class - a collection of queues that keeps track of the total
     * number of items, and provides items eligible for fetching from any queue.
     */
    private static class FetchItemQueues {
        Map<String, FetchItemQueue> queues = Collections
                .synchronizedMap(new LinkedHashMap<String, FetchItemQueue>());

        AtomicInteger inQueues = new AtomicInteger(0);

        final int defaultMaxThread;
        final long crawlDelay;
        final long minCrawlDelay;

        final Config conf;

        public static final String QUEUE_MODE_HOST = "byHost";
        public static final String QUEUE_MODE_DOMAIN = "byDomain";
        public static final String QUEUE_MODE_IP = "byIP";

        String queueMode;

        public FetchItemQueues(Config conf) {
            this.conf = conf;
            this.defaultMaxThread = ConfUtils.getInt(conf,
                    "fetcher.threads.per.queue", 1);
            queueMode = ConfUtils.getString(conf, "fetcher.queue.mode",
                    QUEUE_MODE_HOST);
            // check that the mode is known
            if (!queueMode.equals(QUEUE_MODE_IP)
                    && !queueMode.equals(QUEUE_MODE_DOMAIN)
                    && !queueMode.equals(QUEUE_MODE_HOST)) {
                LOG.error("Unknown partition mode : {} - forcing to byHost",
                        queueMode);
                queueMode = QUEUE_MODE_HOST;
            }
            LOG.info("Using queue mode : {}", queueMode);

            this.crawlDelay = (long) (ConfUtils.getFloat(conf,
                    "fetcher.server.delay", 1.0f) * 1000);
            this.minCrawlDelay = (long) (ConfUtils.getFloat(conf,
                    "fetcher.server.min.delay", 0.0f) * 1000);
        }

        public synchronized void addFetchItem(URL u, Tuple input) {
            FetchItem it = FetchItem.create(u, input, queueMode);
            FetchItemQueue fiq = getFetchItemQueue(it.queueID);
            fiq.addFetchItem(it);
            inQueues.incrementAndGet();
        }

        public synchronized void finishFetchItem(FetchItem it, boolean asap) {
            FetchItemQueue fiq = queues.get(it.queueID);
            if (fiq == null) {
                LOG.warn("Attempting to finish item from unknown queue: {}",
                        it.queueID);
                return;
            }
            fiq.finishFetchItem(it, asap);
        }

        public synchronized FetchItemQueue getFetchItemQueue(String id) {
            FetchItemQueue fiq = queues.get(id);
            if (fiq == null) {
                // custom maxThread value?
                final int customThreadVal = ConfUtils.getInt(conf,
                        "fetcher.maxThreads." + id, defaultMaxThread);
                // initialize queue
                fiq = new FetchItemQueue(conf, customThreadVal, crawlDelay,
                        minCrawlDelay);
                queues.put(id, fiq);
            }
            return fiq;
        }

        public synchronized FetchItem getFetchItem() {
            if (queues.isEmpty()) {
                return null;
            }

            FetchItemQueue start = null;

            do {
                Iterator<Entry<String, FetchItemQueue>> i = queues.entrySet()
                        .iterator();

                if (!i.hasNext()) {
                    return null;
                }

                Map.Entry<String, FetchItemQueue> nextEntry = i.next();

                if (nextEntry == null) {
                    return null;
                }

                FetchItemQueue fiq = nextEntry.getValue();

                // We remove the entry and put it at the end of the map
                i.remove();

                // reap empty queues
                if (fiq.getQueueSize() == 0 && fiq.getInProgressSize() == 0) {
                    continue;
                }

                // Put the entry at the end no matter the result
                queues.put(nextEntry.getKey(), nextEntry.getValue());

                // In case of we are looping
                if (start == null) {
                    start = fiq;
                } else if (fiq == start) {
                    return null;
                }

                FetchItem fit = fiq.getFetchItem();

                if (fit != null) {
                    inQueues.decrementAndGet();
                    return fit;
                }

            } while (!queues.isEmpty());

            return null;
        }
    }

    /**
     * This class picks items from queues and fetches the pages.
     */
    private class FetcherThread extends Thread {

        // longest delay accepted from robots.txt
        private final long maxCrawlDelay;

        public FetcherThread(Config conf) {
            this.setDaemon(true); // don't hang JVM on exit
            this.setName("FetcherThread"); // use an informative name

            this.maxCrawlDelay = ConfUtils.getInt(conf,
                    "fetcher.max.crawl.delay", 30) * 1000;
        }

        @Override
        public void run() {
            FetchItem fit;
            while (true) {
                fit = fetchQueues.getFetchItem();
                if (fit == null) {
                    LOG.debug("{} spin-waiting ...", getName());
                    // spin-wait.
                    spinWaiting.incrementAndGet();
                    try {
                        Thread.sleep(100);
                    } catch (Exception e) {
                    }
                    spinWaiting.decrementAndGet();
                    continue;
                }

                activeThreads.incrementAndGet(); // count threads

                LOG.debug(
                        "[Fetcher #{}] {}  => activeThreads={}, spinWaiting={}, queueID={}",
                        taskID, getName(), activeThreads, spinWaiting,
                        fit.queueID);

                LOG.debug("[Fetcher #{}] {} : Fetching {}", taskID, getName(),
                        fit.url);

                Metadata metadata = null;

                if (fit.t.contains("metadata")) {
                    metadata = (Metadata) fit.t.getValueByField("metadata");
                }
                if (metadata == null) {
                    metadata = Metadata.empty;
                }

                boolean asap = true;

                try {
                    URL URL = new URL(fit.url);
                    Protocol protocol = protocolFactory.getProtocol(URL);

                    if (protocol == null)
                        throw new RuntimeException(
                                "No protocol implementation found for "
                                        + fit.url);

                    BaseRobotRules rules = protocol.getRobotRules(fit.url);

                    // autodiscovery of sitemaps
                    // the sitemaps will be sent down the topology
                    // as many times as there is a URL for a given host
                    // the status updater will certainly cache things
                    // but we could also have a simple cache mechanism here
                    // as well.
                    if (sitemapsAutoDiscovery) {
                        for (String sitemapURL : rules.getSitemaps()) {
                            emitOutlink(fit.t, URL, sitemapURL, metadata,
                                    SiteMapParserBolt.isSitemapKey, "true");
                        }
                    }

                    if (!rules.isAllowed(fit.u.toString())) {
                        LOG.info("Denied by robots.txt: {}", fit.url);
                        // pass the info about denied by robots
                        metadata.setValue(Constants.STATUS_ERROR_CAUSE,
                                "robots.txt");
                        collector
                                .emit(com.digitalpebble.stormcrawler.Constants.StatusStreamName,
                                        fit.t, new Values(fit.url, metadata,
                                                Status.ERROR));
                        continue;
                    }
                    if (rules.getCrawlDelay() > 0) {
                        if (rules.getCrawlDelay() > maxCrawlDelay
                                && maxCrawlDelay >= 0) {
                            LOG.info(
                                    "Crawl-Delay for {} too long ({}), skipping",
                                    fit.url, rules.getCrawlDelay());
                            // pass the info about crawl delay
                            metadata.setValue(Constants.STATUS_ERROR_CAUSE,
                                    "crawl_delay");
                            collector
                                    .emit(com.digitalpebble.stormcrawler.Constants.StatusStreamName,
                                            fit.t, new Values(fit.url,
                                                    metadata, Status.ERROR));
                            continue;
                        } else {
                            FetchItemQueue fiq = fetchQueues
                                    .getFetchItemQueue(fit.queueID);
                            fiq.crawlDelay = rules.getCrawlDelay();
                            LOG.info(
                                    "Crawl delay for queue: {}  is set to {} as per robots.txt. url: {}",
                                    fit.queueID, fiq.crawlDelay, fit.url);
                        }
                    }

                    // will enforce the delay on next fetch
                    asap = false;

                    long start = System.currentTimeMillis();
                    ProtocolResponse response = protocol.getProtocolOutput(
                            fit.url, metadata);
                    long timeFetching = System.currentTimeMillis() - start;

                    final int byteLength = response.getContent().length;

                    averagedMetrics.scope("fetch_time").update(timeFetching);
                    averagedMetrics.scope("bytes_fetched").update(byteLength);
                    perSecMetrics.scope("bytes_fetched_perSec").update(
                            byteLength);
                    perSecMetrics.scope("fetched_perSec").update(1);
                    eventCounter.scope("fetched").incrBy(1);
                    eventCounter.scope("bytes_fetched").incrBy(byteLength);

                    LOG.info(
                            "[Fetcher #{}] Fetched {} with status {} in msec {}",
                            taskID, fit.url, response.getStatusCode(),
                            timeFetching);

                    // passes the input metadata if any to the response one
                    response.getMetadata().putAll(metadata);

                    response.getMetadata().setValue("fetch.statusCode",
                            Integer.toString(response.getStatusCode()));

                    response.getMetadata().setValue("fetch.loadingTime",
                            Long.toString(timeFetching));

                    // determine the status based on the status code
                    final Status status = Status.fromHTTPCode(response
                            .getStatusCode());

                    final Values tupleToSend = new Values(fit.url,
                            response.getMetadata(), status);

                    // if the status is OK emit on default stream
                    if (status.equals(Status.FETCHED)) {
                        if (response.getStatusCode() == 304) {
                            // mark this URL as fetched so that it gets
                            // rescheduled
                            // but do not try to parse or index
                            collector.emit(Constants.StatusStreamName, fit.t,
                                    tupleToSend);
                        } else {
                            // send content for parsing
                            collector.emit(fit.t,
                                    new Values(fit.url, response.getContent(),
                                            response.getMetadata()));
                        }
                    } else if (status.equals(Status.REDIRECTION)) {

                        // find the URL it redirects to
                        String redirection = response.getMetadata()
                                .getFirstValue(HttpHeaders.LOCATION);

                        // stores the URL it redirects to
                        // used for debugging mainly - do not resolve the target
                        // URL
                        if (StringUtils.isNotBlank(redirection)) {
                            response.getMetadata().setValue("_redirTo",
                                    redirection);
                        }

                        // mark this URL as redirected
                        collector.emit(Constants.StatusStreamName, fit.t,
                                tupleToSend);

                        if (allowRedirs()
                                && StringUtils.isNotBlank(redirection)) {
                            emitOutlink(fit.t, URL, redirection,
                                    response.getMetadata());
                        }
                    }
                    // error
                    else {
                        collector.emit(Constants.StatusStreamName, fit.t,
                                tupleToSend);
                    }

                } catch (Exception exece) {
                    String message = exece.getMessage();
                    if (message == null)
                        message = "";

                    // common exceptions for which we log only a short message
                    if (exece.getCause() instanceof java.util.concurrent.TimeoutException
                            || message.contains(" timed out")) {
                        LOG.error("Socket timeout fetching {}", fit.url);
                        message = "Socket timeout fetching";
                    } else if (exece.getCause() instanceof java.net.UnknownHostException
                            || exece instanceof java.net.UnknownHostException) {
                        LOG.error("Unknown host {}", fit.url);
                        message = "Unknown host";
                    } else {
                        LOG.error("Exception while fetching {}", fit.url, exece);
                        message = exece.getClass().getName();
                    }

                    if (metadata.size() == 0) {
                        metadata = new Metadata();
                    }
                    // add the reason of the failure in the metadata
                    metadata.setValue("fetch.exception", message);

                    // send to status stream
                    collector.emit(Constants.StatusStreamName, fit.t,
                            new Values(fit.url, metadata, Status.FETCH_ERROR));

                    eventCounter.scope("exception").incrBy(1);
                } finally {
                    fetchQueues.finishFetchItem(fit, asap);
                    activeThreads.decrementAndGet(); // count threads
                    // ack it whatever happens
                    collector.ack(fit.t);
                }
            }

        }
    }

    private void checkConfiguration(Config stormConf) {

        // ensure that a value has been set for the agent name and that that
        // agent name is the first value in the agents we advertise for robot
        // rules parsing
        String agentName = (String) stormConf.get("http.agent.name");
        if (agentName == null || agentName.trim().length() == 0) {
            String message = "Fetcher: No agents listed in 'http.agent.name'"
                    + " property.";
            LOG.error(message);
            throw new IllegalArgumentException(message);
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {

        super.prepare(stormConf, context, collector);

        Config conf = new Config();
        conf.putAll(stormConf);

        checkConfiguration(conf);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss",
                Locale.ENGLISH);
        long start = System.currentTimeMillis();
        LOG.info("[Fetcher #{}] : starting at {}", taskID, sdf.format(start));

        int metricsTimeBucketSecs = ConfUtils.getInt(conf,
                "fetcher.metrics.time.bucket.secs", 10);

        // Register a "MultiCountMetric" to count different events in this bolt
        // Storm will emit the counts every n seconds to a special bolt via a
        // system stream
        // The data can be accessed by registering a "MetricConsumer" in the
        // topology
        this.eventCounter = context.registerMetric("fetcher_counter",
                new MultiCountMetric(), metricsTimeBucketSecs);

        // create gauges
        context.registerMetric("activethreads", new IMetric() {
            @Override
            public Object getValueAndReset() {
                return activeThreads.get();
            }
        }, metricsTimeBucketSecs);

        context.registerMetric("in_queues", new IMetric() {
            @Override
            public Object getValueAndReset() {
                return fetchQueues.inQueues.get();
            }
        }, metricsTimeBucketSecs);

        context.registerMetric("num_queues", new IMetric() {
            @Override
            public Object getValueAndReset() {
                return fetchQueues.queues.size();
            }
        }, metricsTimeBucketSecs);

        this.averagedMetrics = context.registerMetric("fetcher_average_perdoc",
                new MultiReducedMetric(new MeanReducer()),
                metricsTimeBucketSecs);

        this.perSecMetrics = context.registerMetric("fetcher_average_persec",
                new MultiReducedMetric(new PerSecondReducer()),
                metricsTimeBucketSecs);

        protocolFactory = new ProtocolFactory(conf);

        this.fetchQueues = new FetchItemQueues(conf);

        this.taskID = context.getThisTaskId();

        int threadCount = ConfUtils.getInt(conf, "fetcher.threads.number", 10);
        for (int i = 0; i < threadCount; i++) { // spawn threads
            new FetcherThread(conf).start();
        }

        sitemapsAutoDiscovery = ConfUtils.getBoolean(stormConf,
                "sitemap.discovery", false);

        maxNumberURLsInQueues = ConfUtils.getInt(conf,
                "fetcher.max.urls.in.queues", -1);

        /**
         * If set to a valid path e.g. /tmp/fetcher-dump-{port} on a worker
         * node, the content of the queues will be dumped to the logs for
         * debugging. The port number needs to match the one used by the
         * FetcherBolt instance.
         **/
        String debugfiletriggerpattern = ConfUtils.getString(conf,
                "fetcherbolt.queue.debug.filepath");

        if (StringUtils.isNotBlank(debugfiletriggerpattern)) {
            debugfiletrigger = new File(
                    debugfiletriggerpattern.replaceAll("\\{port\\}",
                            Integer.toString(context.getThisWorkerPort())));
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        super.declareOutputFields(declarer);
        declarer.declare(new Fields("url", "content", "metadata"));
    }

    @Override
    public void cleanup() {
        protocolFactory.cleanup();
    }

    @Override
    public void execute(Tuple input) {
        boolean toomanyurlsinqueues = false;
        do {
            if (this.maxNumberURLsInQueues != -1
                    && (this.activeThreads.get() + this.fetchQueues.inQueues
                            .get()) >= maxNumberURLsInQueues) {
                toomanyurlsinqueues = true;
                try {
                    Thread.currentThread().sleep(500);
                } catch (InterruptedException e) {
                }
            }
            LOG.info("[Fetcher #{}] Threads : {}\tqueues : {}\tin_queues : {}",
                    taskID, this.activeThreads.get(),
                    this.fetchQueues.queues.size(),
                    this.fetchQueues.inQueues.get());
        } while (toomanyurlsinqueues);

        // detect whether there is a file indicating that we should
        // dump the content of the queues to the log
        if (debugfiletrigger != null && debugfiletrigger.exists()) {
            LOG.info("Found trigger file {}", debugfiletrigger);
            logQueuesContent();
            debugfiletrigger.delete();
        }

        String urlString = input.getStringByField("url");
        URL url;

        if (StringUtils.isBlank(urlString)) {
            LOG.info("[Fetcher #{}] Missing value for field url in tuple {}",
                    taskID, input);
            // ignore silently
            collector.ack(input);
            return;
        }

        try {
            url = new URL(urlString);
        } catch (MalformedURLException e) {
            LOG.error("{} is a malformed URL", urlString);

            Metadata metadata = (Metadata) input.getValueByField("metadata");
            if (metadata == null) {
                metadata = new Metadata();
            }
            // Report to status stream and ack
            metadata.setValue(Constants.STATUS_ERROR_CAUSE, "malformed URL");
            collector.emit(
                    com.digitalpebble.stormcrawler.Constants.StatusStreamName,
                    input, new Values(urlString, metadata, Status.ERROR));
            collector.ack(input);
            return;
        }

        fetchQueues.addFetchItem(url, input);
    }

    private void logQueuesContent() {
        StringBuilder sb = new StringBuilder();
        synchronized (fetchQueues.queues) {
            sb.append("\nNum queues : ").append(fetchQueues.queues.size());
            Iterator<Entry<String, FetchItemQueue>> iterator = fetchQueues.queues
                    .entrySet().iterator();
            while (iterator.hasNext()) {
                Entry<String, FetchItemQueue> entry = iterator.next();
                sb.append("\nQueue ID : ").append(entry.getKey());
                FetchItemQueue fiq = entry.getValue();
                sb.append("\t size : ").append(fiq.getQueueSize());
                sb.append("\t in progress : ").append(fiq.getInProgressSize());
                Iterator<FetchItem> urlsIter = fiq.queue.iterator();
                while (urlsIter.hasNext()) {
                    sb.append("\n\t").append(urlsIter.next().url);
                }
            }
            LOG.info("Dumping queue content {}", sb.toString());
        }
    }

}
