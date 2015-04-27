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

import java.io.IOException;
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
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import backtype.storm.metric.api.MeanReducer;
import backtype.storm.metric.api.MultiReducedMetric;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.guava.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.metric.api.CountMetric;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.digitalpebble.storm.crawler.Metadata;
import com.digitalpebble.storm.crawler.filtering.URLFilters;
import com.digitalpebble.storm.crawler.persistence.Status;
import com.digitalpebble.storm.crawler.protocol.HttpHeaders;
import com.digitalpebble.storm.crawler.protocol.Protocol;
import com.digitalpebble.storm.crawler.protocol.ProtocolFactory;
import com.digitalpebble.storm.crawler.protocol.ProtocolResponse;
import com.digitalpebble.storm.crawler.util.ConfUtils;
import com.digitalpebble.storm.crawler.util.MetadataTransfer;
import com.digitalpebble.storm.crawler.util.URLUtil;

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
    private MultiCountMetric metricGauge;
    private MultiReducedMetric averagedMetrics;

    private ProtocolFactory protocolFactory;

    private final List<Tuple> ackQueue = Collections
            .synchronizedList(new LinkedList<Tuple>());

    private final List<Object[]> emitQueue = Collections
            .synchronizedList(new LinkedList<Object[]>());

    private int taskIndex = -1;

    private URLFilters urlFilters;

    private boolean allowRedirs;

    private MetadataTransfer metadataTransfer;

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

        public static FetchItem create(Tuple t, String queueMode) {

            String url = t.getStringByField("url");

            String queueID;
            URL u = null;
            try {
                u = new URL(url.toString());
            } catch (Exception e) {
                LOG.warn("Cannot parse url: {}", url, e);
                return null;
            }

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
                    // unable to resolve it, so don't fall back to host name
                    LOG.warn("Unable to resolve: {}, skipping.", u.getHost());
                    return null;
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
                if (key == null) {
                    LOG.warn(
                            "Unknown host for url: {}, using URL string as key",
                            url);
                    key = u.toExternalForm();
                }
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
        Deque<FetchItem> queue = new LinkedBlockingDeque<FetcherBolt.FetchItem>();

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

        public boolean addFetchItem(FetchItem it) {
            if (it == null)
                return false;
            return queue.add(it);
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

        Map<String, FetchItemQueue> queues = new LinkedHashMap<String, FetchItemQueue>();
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

        public synchronized void addFetchItem(Tuple input) {
            FetchItem it = FetchItem.create(input, queueMode);
            if (it != null)
                addFetchItem(it);
        }

        public synchronized void addFetchItem(FetchItem it) {
            FetchItemQueue fiq = getFetchItemQueue(it.queueID);
            boolean added = fiq.addFetchItem(it);
            if (added)
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

        // TODO longest delay accepted from robots.txt
        private final long maxCrawlDelay;

        public FetcherThread(Config conf) {
            this.setDaemon(true); // don't hang JVM on exit
            this.setName("FetcherThread"); // use an informative name

            this.maxCrawlDelay = ConfUtils.getInt(conf,
                    "fetcher.max.crawl.delay", 30) * 1000;
        }

        @Override
        public void run() {
            FetchItem fit = null;
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
                        taskIndex, getName(), activeThreads, spinWaiting,
                        fit.queueID);

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
                    if (!rules.isAllowed(fit.u.toString())) {

                        LOG.info("Denied by robots.txt: {}", fit.url);

                        // TODO pass the info about denied by robots
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

                            // TODO pass the info about crawl delay
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

                    averagedMetrics.scope("fetch_time").update(System.currentTimeMillis() - start);
                    averagedMetrics.scope("bytes_fetched").update(response.getContent().length);

                    LOG.info("[Fetcher #{}] Fetched {} with status {}",
                            taskIndex, fit.url, response.getStatusCode());

                    eventCounter.scope("fetched").incrBy(1);

                    response.getMetadata().setValue("fetch.statusCode",
                            Integer.toString(response.getStatusCode()));

                    // update the stats
                    // eventStats.scope("KB downloaded").update((long)
                    // content.length / 1024l);
                    // eventStats.scope("# pages").update(1);

                    // passes the input metadata if any to the response one
                    response.getMetadata().putAll(metadata);

                    // determine the status based on the status code
                    Status status = Status.fromHTTPCode(response
                            .getStatusCode());

                    // if the status is OK emit on default stream
                    if (status.equals(Status.FETCHED)) {
                        // do not reparse the content if it hasn't changed
                        if (response.getStatusCode() != 304) {
                            emitQueue.add(new Object[] {
                                    Utils.DEFAULT_STREAM_ID,
                                    fit.t,
                                    new Values(fit.url, response.getContent(),
                                            response.getMetadata()) });
                        }
                    } else if (status.equals(Status.REDIRECTION)) {
                        // mark this URL as redirected
                        emitQueue
                                .add(new Object[] {
                                        com.digitalpebble.storm.crawler.Constants.StatusStreamName,
                                        fit.t,
                                        new Values(fit.url, response
                                                .getMetadata(), status) });

                        // find the URL it redirects to
                        String[] redirection = response.getMetadata()
                                .getValues(HttpHeaders.LOCATION);

                        if (allowRedirs && redirection != null
                                && redirection.length != 0
                                && redirection[0] != null) {
                            handleRedirect(fit.t, fit.url, redirection[0],
                                    response.getMetadata());
                        }

                    }
                    // error
                    else {
                        emitQueue
                                .add(new Object[] {
                                        com.digitalpebble.storm.crawler.Constants.StatusStreamName,
                                        fit.t,
                                        new Values(fit.url, response
                                                .getMetadata(), status) });
                    }

                } catch (Exception exece) {
                    String message = exece.getMessage();
                    if (message == null)
                        message = "";
                    if (exece.getCause() instanceof java.util.concurrent.TimeoutException)
                        LOG.error("Socket timeout fetching {}", fit.url);
                    else if (message.contains("connection timed out"))
                        LOG.error("Socket timeout fetching {}", fit.url);
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

                    eventCounter.scope("fetch exception").incrBy(1);
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

    private void handleRedirect(Tuple t, String sourceUrl, String newUrl,
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
        if (this.urlFilters != null) {
            newUrl = this.urlFilters.filter(sURL, sourceMetadata, newUrl);
        }

        // filtered
        if (newUrl == null) {
            return;
        }

        Metadata metadata = metadataTransfer.getMetaForOutlink(newUrl,
                sourceUrl, sourceMetadata);

        // TODO check that hasn't exceeded max number of redirections

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
        LOG.info("[Fetcher #{}] : starting at {}", taskIndex, sdf.format(start));

        // Register a "MultiCountMetric" to count different events in this bolt
        // Storm will emit the counts every n seconds to a special bolt via a
        // system stream
        // The data can be accessed by registering a "MetricConsumer" in the
        // topology
        this.eventCounter = context.registerMetric("fetcher_counter",
                new MultiCountMetric(), 10);

        this.metricGauge = context.registerMetric("fetcher",
                new MultiCountMetric(), 10);

        this.averagedMetrics = context.registerMetric("fetcher_average",
                new MultiReducedMetric(new MeanReducer()), 10);

        protocolFactory = new ProtocolFactory(conf);

        this.fetchQueues = new FetchItemQueues(conf);

        this.taskIndex = context.getThisTaskIndex();

        int threadCount = ConfUtils.getInt(conf, "fetcher.threads.number", 10);
        for (int i = 0; i < threadCount; i++) { // spawn threads
            new FetcherThread(conf).start();
        }

        String urlconfigfile = ConfUtils.getString(conf,
                "urlfilters.config.file", "urlfilters.json");

        if (urlconfigfile != null)
            try {
                urlFilters = new URLFilters(conf, urlconfigfile);
            } catch (IOException e) {
                LOG.error("Exception caught while loading the URLFilters");
                throw new RuntimeException(
                        "Exception caught while loading the URLFilters", e);
            }

        allowRedirs = ConfUtils.getBoolean(stormConf,
                com.digitalpebble.storm.crawler.Constants.AllowRedirParamName,
                true);

        metadataTransfer = MetadataTransfer.getInstance(stormConf);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url", "content", "metadata"));
        declarer.declareStream(
                com.digitalpebble.storm.crawler.Constants.StatusStreamName,
                new Fields("url", "metadata", "status"));
    }

    private boolean isTickTuple(Tuple tuple) {
        String sourceComponent = tuple.getSourceComponent();
        String sourceStreamId = tuple.getSourceStreamId();
        return sourceComponent.equals(Constants.SYSTEM_COMPONENT_ID)
                && sourceStreamId.equals(Constants.SYSTEM_TICK_STREAM_ID);
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

        int acked = 0;
        int emitted = 0;

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
            LOG.info("[Fetcher #{}] Acked : {}\tEmitted : {}", taskIndex,
                    acked, emitted);
    }

    @Override
    public void execute(Tuple input) {

        // triggered by the arrival of a tuple
        // be it a tick or normal one
        flushQueues();

        if (isTickTuple(input)) {
            return;
        }

        CountMetric metric = metricGauge.scope("activethreads");
        metric.getValueAndReset();
        metric.incrBy(this.activeThreads.get());

        metric = metricGauge.scope("in queues");
        metric.getValueAndReset();
        metric.incrBy(this.fetchQueues.inQueues.get());

        metric = metricGauge.scope("queues");
        metric.getValueAndReset();
        metric.incrBy(this.fetchQueues.queues.size());

        LOG.info("[Fetcher #{}] Threads : {}\tqueues : {}\tin_queues : {}",
                taskIndex, this.activeThreads.get(),
                this.fetchQueues.queues.size(), this.fetchQueues.inQueues.get());

        if (!input.contains("url")) {
            LOG.info("[Fetcher #{}] Missing field url in tuple {}", taskIndex,
                    input);
            // ignore silently
            _collector.ack(input);
            return;
        }

        String url = input.getStringByField("url");

        // has one but what about the content?
        if (StringUtils.isBlank(url)) {
            LOG.info("[Fetcher #{}] Missing value for field url in tuple {}",
                    taskIndex, input);
            // ignore silently
            _collector.ack(input);
            return;
        }

        fetchQueues.addFetchItem(input);
    }
}
