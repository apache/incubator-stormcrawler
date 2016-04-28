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

package com.digitalpebble.storm.crawler.bolt;

import java.io.File;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.storm.crawler.Metadata;
import com.digitalpebble.storm.crawler.filtering.URLFilters;
import com.digitalpebble.storm.crawler.persistence.Status;
import com.digitalpebble.storm.crawler.protocol.HttpHeaders;
import com.digitalpebble.storm.crawler.protocol.Protocol;
import com.digitalpebble.storm.crawler.protocol.ProtocolFactory;
import com.digitalpebble.storm.crawler.protocol.ProtocolResponse;
import com.digitalpebble.storm.crawler.util.ConfUtils;
import com.digitalpebble.storm.crawler.util.MetadataTransfer;
import com.digitalpebble.storm.crawler.util.PerSecondReducer;
import com.digitalpebble.storm.crawler.util.URLUtil;
import com.google.common.collect.Iterables;

import backtype.storm.Config;
import backtype.storm.metric.api.IMetric;
import backtype.storm.metric.api.MeanReducer;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.metric.api.MultiReducedMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.TupleUtils;
import backtype.storm.utils.Utils;
import crawlercommons.robots.BaseRobotRules;
import crawlercommons.url.PaidLevelDomain;

/**
 * A multithreaded, queue-based fetcher adapted from Apache Nutch. Enforces the
 * politeness and handles the fetching threads itself.
 */
@SuppressWarnings("serial")
public class FetcherBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory
            .getLogger(FetcherBolt.class);

    private final AtomicInteger activeThreads = new AtomicInteger(0);
    private final AtomicInteger spinWaiting = new AtomicInteger(0);

    private FetchItemQueues fetchQueues;
    private OutputCollector _collector;

    private MultiCountMetric eventCounter;
    private MultiReducedMetric averagedMetrics;

    private ProtocolFactory protocolFactory;

    private final List<Tuple> ackQueue = Collections
            .synchronizedList(new LinkedList<Tuple>());

    private final List<Object[]> emitQueue = Collections
            .synchronizedList(new LinkedList<Object[]>());

    private int taskID = -1;

    private URLFilters urlFilters;

    private boolean allowRedirs;

    boolean sitemapsAutoDiscovery = false;

    private MetadataTransfer metadataTransfer;

    private MultiReducedMetric perSecMetrics;

    private File debugfiletrigger;

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
            if (queue.size() == 0)
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

        Map<String, FetchItemQueue> queues = new LinkedHashMap<>();
        Iterator<String> it = Iterables.cycle(queues.keySet()).iterator();

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

                // Reset the cyclic iterator to start of the list.
                it = Iterables.cycle(queues.keySet()).iterator();
            }
            return fiq;
        }

        public synchronized FetchItem getFetchItem() {

            if (queues.isEmpty() || !it.hasNext())
                return null;

            FetchItemQueue start = null;

            do {
                FetchItemQueue fiq = queues.get(it.next());
                // reap empty queues
                if (fiq.getQueueSize() == 0 && fiq.getInProgressSize() == 0) {
                    it.remove();
                    continue;
                }
                // means that we have traversed the
                // entire list and yet couldn't find any
                // eligible fetch item

                if (start == null)
                    start = fiq;
                else if (start == fiq)
                    return null;

                FetchItem fit = fiq.getFetchItem();
                if (fit != null) {
                    inQueues.decrementAndGet();
                    return fit;
                }
            } while (it.hasNext());
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

                LOG.info(
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
                    Protocol protocol = protocolFactory.getProtocol(new URL(
                            fit.url));

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
                            handleOutlink(fit.t, fit.url, sitemapURL, metadata);
                        }
                    }

                    if (!rules.isAllowed(fit.u.toString())) {

                        LOG.info("Denied by robots.txt: {}", fit.url);

                        // pass the info about denied by robots
                        metadata.setValue("error.cause", "robots.txt");

                        emitQueue
                                .add(new Object[] {
                                        com.digitalpebble.storm.crawler.Constants.StatusStreamName,
                                        fit.t,
                                        new Values(fit.url, metadata,
                                                Status.ERROR) });
                        continue;
                    }
                    if (rules.getCrawlDelay() > 0) {
                        if (rules.getCrawlDelay() > maxCrawlDelay
                                && maxCrawlDelay >= 0) {

                            LOG.info(
                                    "Crawl-Delay for {} too long ({}), skipping",
                                    fit.url, rules.getCrawlDelay());

                            // pass the info about crawl delay
                            metadata.setValue("error.cause", "crawl_delay");

                            emitQueue
                                    .add(new Object[] {
                                            com.digitalpebble.storm.crawler.Constants.StatusStreamName,
                                            fit.t,
                                            new Values(fit.url, metadata,
                                                    Status.ERROR) });
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

                    response.getMetadata().setValue("fetch.statusCode",
                            Integer.toString(response.getStatusCode()));

                    response.getMetadata().setValue("fetch.loadingTime",
                            Long.toString(timeFetching));

                    // passes the input metadata if any to the response one
                    response.getMetadata().putAll(metadata);

                    // determine the status based on the status code
                    final Status status = Status.fromHTTPCode(response
                            .getStatusCode());

                    final Object[] statusToSend = new Object[] {
                            com.digitalpebble.storm.crawler.Constants.StatusStreamName,
                            fit.t,
                            new Values(fit.url, response.getMetadata(), status) };

                    // if the status is OK emit on default stream
                    if (status.equals(Status.FETCHED)) {
                        if (response.getStatusCode() == 304) {
                            // mark this URL as fetched so that it gets
                            // rescheduled
                            // but do not try to parse or index
                            emitQueue.add(statusToSend);
                        } else {
                            // send content for parsing
                            emitQueue.add(new Object[] {
                                    Utils.DEFAULT_STREAM_ID,
                                    fit.t,
                                    new Values(fit.url, response.getContent(),
                                            response.getMetadata()) });
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
                        emitQueue.add(statusToSend);

                        if (allowRedirs && StringUtils.isNotBlank(redirection)) {
                            handleOutlink(fit.t, fit.url, redirection,
                                    response.getMetadata());
                        }

                    }
                    // error
                    else {
                        emitQueue.add(statusToSend);
                    }

                } catch (Exception exece) {
                    String message = exece.getMessage();
                    if (message == null)
                        message = "";

                    // common exceptions for which we log only a short message
                    if (exece.getCause() instanceof java.util.concurrent.TimeoutException)
                        LOG.error("Socket timeout fetching {}", fit.url);
                    else if (message.contains(" timed out"))
                        LOG.error("Socket timeout fetching {}", fit.url);
                    else if (exece.getCause() instanceof java.net.UnknownHostException
                            | exece instanceof java.net.UnknownHostException)
                        LOG.error("Unknown host {}", fit.url);
                    else
                        LOG.error("Exception while fetching {}", fit.url, exece);

                    if (metadata.size() == 0) {
                        metadata = new Metadata();
                    }
                    // add the reason of the failure in the metadata
                    metadata.setValue("fetch.exception", message);

                    // send to status stream
                    emitQueue
                            .add(new Object[] {
                                    com.digitalpebble.storm.crawler.Constants.StatusStreamName,
                                    fit.t,
                                    new Values(fit.url, metadata,
                                            Status.FETCH_ERROR) });

                    eventCounter.scope("exception").incrBy(1);
                } finally {
                    fetchQueues.finishFetchItem(fit, asap);
                    activeThreads.decrementAndGet(); // count threads
                    // ack it whatever happens
                    synchronized (ackQueue) {
                        ackQueue.add(fit.t);
                    }
                }
            }

        }
    }

    private void handleOutlink(Tuple t, String sourceUrl, String newUrl,
            Metadata sourceMetadata) {

        // build an absolute URL
        URL sURL;
        try {
            sURL = new URL(sourceUrl);
            URL tmpURL = URLUtil.resolveURL(sURL, newUrl);
            newUrl = tmpURL.toExternalForm();
        } catch (MalformedURLException e) {
            LOG.debug("MalformedURLException on {} or {}: {}", sourceUrl,
                    newUrl, e);
            return;
        }

        // apply URL filters
        newUrl = this.urlFilters.filter(sURL, sourceMetadata, newUrl);

        // filtered
        if (newUrl == null) {
            return;
        }

        Metadata metadata = metadataTransfer.getMetaForOutlink(newUrl,
                sourceUrl, sourceMetadata);

        emitQueue.add(new Object[] {
                com.digitalpebble.storm.crawler.Constants.StatusStreamName, t,
                new Values(newUrl, metadata, Status.DISCOVERED) });
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

        _collector = collector;

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

        urlFilters = URLFilters.fromConf(stormConf);

        allowRedirs = ConfUtils.getBoolean(stormConf,
                com.digitalpebble.storm.crawler.Constants.AllowRedirParamName,
                true);

        sitemapsAutoDiscovery = ConfUtils.getBoolean(stormConf,
                "sitemap.discovery", false);

        metadataTransfer = MetadataTransfer.getInstance(stormConf);

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
        declarer.declare(new Fields("url", "content", "metadata"));
        declarer.declareStream(
                com.digitalpebble.storm.crawler.Constants.StatusStreamName,
                new Fields("url", "metadata", "status"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 1);
        return conf;
    }

    private void flushQueues() {
        // main thread in charge of acking and failing
        // see
        // https://github.com/nathanmarz/storm/wiki/Troubleshooting#nullpointerexception-from-deep-inside-storm

        int acked;
        int emitted;

        // emit with or without anchors
        // before acking
        synchronized (emitQueue) {
            for (Object[] toemit : this.emitQueue) {
                String streamID = (String) toemit[0];
                Tuple anchor = (Tuple) toemit[1];
                Values vals = (Values) toemit[2];
                if (anchor == null)
                    _collector.emit(streamID, vals);
                else
                    _collector.emit(streamID, Arrays.asList(anchor), vals);
            }
            emitted = emitQueue.size();
            emitQueue.clear();
        }

        // have a tick tuple to make sure we don't get starved
        synchronized (ackQueue) {
            for (Tuple toack : this.ackQueue) {
                _collector.ack(toack);
            }
            acked = ackQueue.size();
            ackQueue.clear();
        }

        if (acked + emitted > 0)
            LOG.info("[Fetcher #{}] Acked : {}\tEmitted : {}", taskID, acked,
                    emitted);
    }

    @Override
    public void execute(Tuple input) {

        // triggered by the arrival of a tuple
        // be it a tick or normal one
        flushQueues();

        LOG.info("[Fetcher #{}] Threads : {}\tqueues : {}\tin_queues : {}",
                taskID, this.activeThreads.get(),
                this.fetchQueues.queues.size(), this.fetchQueues.inQueues.get());

        // detect whether there is a file indicating that we should
        // dump the content of the queues to the log
        if (debugfiletrigger != null && debugfiletrigger.exists()) {
            LOG.info("Found trigger file {}", debugfiletrigger);
            logQueuesContent();
            debugfiletrigger.delete();
        }

        if (TupleUtils.isTick(input)) {
            _collector.ack(input);
            return;
        }

        String urlString = input.getStringByField("url");
        URL url;

        if (StringUtils.isBlank(urlString)) {
            LOG.info("[Fetcher #{}] Missing value for field url in tuple {}",
                    taskID, input);
            // ignore silently
            _collector.ack(input);
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
            metadata.setValue("error.cause", "malformed URL");
            _collector.emit(
                    com.digitalpebble.storm.crawler.Constants.StatusStreamName,
                    input, new Values(urlString, metadata, Status.ERROR));
            _collector.ack(input);
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
