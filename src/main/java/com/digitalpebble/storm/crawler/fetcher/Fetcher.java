/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
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
package com.digitalpebble.storm.crawler.fetcher;

import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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

import com.digitalpebble.storm.crawler.util.Configuration;
import com.digitalpebble.storm.crawler.StormConfiguration;

import crawlercommons.url.PaidLevelDomain;

/**
 * A multithreaded, queue-based fetcher adapted from Apache Nutch.
 **/

public class Fetcher extends BaseRichBolt {

    public static final Logger LOG = LoggerFactory.getLogger(Fetcher.class);

    private AtomicInteger activeThreads = new AtomicInteger(0);
    private AtomicInteger spinWaiting = new AtomicInteger(0);

    private FetchItemQueues fetchQueues;
    private Configuration conf;
    private OutputCollector _collector;

    private static MultiCountMetric eventCounter;
    private static MultiCountMetric metricGauge;

    private ProtocolFactory protocolFactory;

    // threads must not access collector
    // directly

    private final List<Tuple> ackQueue = Collections
            .synchronizedList(new LinkedList<Tuple>());
    private final List<Tuple> failQueue = Collections
            .synchronizedList(new LinkedList<Tuple>());

    private final List<Object[]> emitQueue = Collections
            .synchronizedList(new LinkedList<Object[]>());

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
                LOG.warn("Cannot parse url: " + url, e);
                return null;
            }
            final String proto = u.getProtocol().toLowerCase();
            String key;
            if (FetchItemQueues.QUEUE_MODE_IP.equalsIgnoreCase(queueMode)) {
                if (t.contains("ip")) {
                    key = t.getStringByField("ip");
                } else {
                    try {
                        final InetAddress addr = InetAddress.getByName(u
                                .getHost());
                        key = addr.getHostAddress();
                    } catch (final UnknownHostException e) {
                        // unable to resolve it, so don't fall back to host name
                        LOG.warn("Unable to resolve: " + u.getHost()
                                + ", skipping.");
                        return null;
                    }
                }
            } else if (FetchItemQueues.QUEUE_MODE_DOMAIN
                    .equalsIgnoreCase(queueMode)) {
                key = PaidLevelDomain.getPLD(u);
                if (key == null) {
                    LOG.warn("Unknown domain for url: " + url
                            + ", using URL string as key");
                    key = u.toExternalForm();
                }
            } else {
                key = u.getHost();
                if (key == null) {
                    LOG.warn("Unknown host for url: " + url
                            + ", using URL string as key");
                    key = u.toExternalForm();
                }
            }
            queueID = proto + "://" + key.toLowerCase();
            return new FetchItem(url, u, t, queueID);
        }

    }

    /**
     * This class handles FetchItems which come from the same host ID (be it a
     * proto/hostname or proto/IP pair). It also keeps track of requests in
     * progress and elapsed time between requests.
     */
    private static class FetchItemQueue {
        Deque<FetchItem> queue = new LinkedBlockingDeque<Fetcher.FetchItem>();
        Set<FetchItem> inProgress = Collections
                .synchronizedSet(new HashSet<FetchItem>());
        AtomicLong nextFetchTime = new AtomicLong();
        long crawlDelay;
        long minCrawlDelay;
        int maxThreads;
        Configuration conf;

        public FetchItemQueue(Configuration conf, int maxThreads,
                long crawlDelay, long minCrawlDelay) {
            this.conf = conf;
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
            return inProgress.size();
        }

        public void finishFetchItem(FetchItem it, boolean asap) {
            if (it != null) {
                inProgress.remove(it);
                setEndTime(System.currentTimeMillis(), asap);
            }
        }

        public void addFetchItem(FetchItem it) {
            if (it == null)
                return;
            queue.add(it);
        }

        public FetchItem getFetchItem() {
            if (inProgress.size() >= maxThreads)
                return null;
            long now = System.currentTimeMillis();
            if (nextFetchTime.get() > now)
                return null;
            FetchItem it = null;
            if (queue.size() == 0)
                return null;
            try {
                it = queue.removeFirst();
                inProgress.add(it);
            } catch (Exception e) {
                LOG.error(
                        "Cannot remove FetchItem from queue or cannot add it to inProgress queue",
                        e);
            }
            return it;
        }

        public synchronized void dump() {
            LOG.info("  maxThreads    = " + maxThreads);
            LOG.info("  inProgress    = " + inProgress.size());
            LOG.info("  crawlDelay    = " + crawlDelay);
            LOG.info("  minCrawlDelay = " + minCrawlDelay);
            LOG.info("  nextFetchTime = " + nextFetchTime.get());
            LOG.info("  now           = " + System.currentTimeMillis());
            Iterator<FetchItem> iter = queue.iterator();
            int i = 0;
            while (iter.hasNext()) {
                FetchItem it = iter.next();
                LOG.info("  " + i + ". " + it.url);
                i++;
            }
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
        public static final String DEFAULT_ID = "default";
        Map<String, FetchItemQueue> queues = new HashMap<String, FetchItemQueue>();
        AtomicInteger inQueues = new AtomicInteger(0);
        int maxThreads;
        long crawlDelay;
        long minCrawlDelay;

        Configuration conf;

        public static final String QUEUE_MODE_HOST = "byHost";
        public static final String QUEUE_MODE_DOMAIN = "byDomain";
        public static final String QUEUE_MODE_IP = "byIP";

        String queueMode;

        public FetchItemQueues(Configuration conf) {
            this.conf = conf;
            this.maxThreads = conf.getInt("fetcher.threads.per.queue", 1);
            queueMode = conf.get("fetcher.queue.mode", QUEUE_MODE_HOST);
            // check that the mode is known
            if (!queueMode.equals(QUEUE_MODE_IP)
                    && !queueMode.equals(QUEUE_MODE_DOMAIN)
                    && !queueMode.equals(QUEUE_MODE_HOST)) {
                LOG.error("Unknown partition mode : " + queueMode
                        + " - forcing to byHost");
                queueMode = QUEUE_MODE_HOST;
            }
            LOG.info("Using queue mode : " + queueMode);

            this.crawlDelay = (long) (conf.getFloat("fetcher.server.delay",
                    1.0f) * 1000);
            this.minCrawlDelay = (long) (conf.getFloat(
                    "fetcher.server.min.delay", 0.0f) * 1000);
        }

        public void addFetchItem(Tuple input) {
            FetchItem it = FetchItem.create(input, queueMode);
            if (it != null)
                addFetchItem(it);
        }

        public synchronized void addFetchItem(FetchItem it) {
            FetchItemQueue fiq = getFetchItemQueue(it.queueID);
            fiq.addFetchItem(it);
            inQueues.incrementAndGet();
        }

        public void finishFetchItem(FetchItem it) {
            finishFetchItem(it, false);
        }

        public void finishFetchItem(FetchItem it, boolean asap) {
            FetchItemQueue fiq = queues.get(it.queueID);
            if (fiq == null) {
                LOG.warn("Attempting to finish item from unknown queue: " + it);
                return;
            }
            fiq.finishFetchItem(it, asap);
        }

        public synchronized FetchItemQueue getFetchItemQueue(String id) {
            FetchItemQueue fiq = queues.get(id);
            if (fiq == null) {
                // initialize queue
                fiq = new FetchItemQueue(conf, maxThreads, crawlDelay,
                        minCrawlDelay);
                eventCounter.scope("queues").incrBy(1);
                queues.put(id, fiq);
            }
            return fiq;
        }

        public synchronized FetchItem getFetchItem() {
            Iterator<Map.Entry<String, FetchItemQueue>> it = queues.entrySet()
                    .iterator();
            while (it.hasNext()) {
                FetchItemQueue fiq = it.next().getValue();
                // reap empty queues
                if (fiq.getQueueSize() == 0 && fiq.getInProgressSize() == 0) {
                    it.remove();
                    eventCounter.scope("queues").incrBy(-1);
                    continue;
                }
                FetchItem fit = fiq.getFetchItem();
                if (fit != null) {
                    inQueues.decrementAndGet();
                    return fit;
                }
            }
            return null;
        }

    }

    /**
     * This class picks items from queues and fetches the pages.
     */
    private class FetcherThread extends Thread {

        // TODO longest delay accepted from robots.txt
        private long maxCrawlDelay;

        public FetcherThread(Configuration conf) {
            this.setDaemon(true); // don't hang JVM on exit
            this.setName("FetcherThread"); // use an informative name

            this.maxCrawlDelay = conf.getInt("fetcher.max.crawl.delay", 30) * 1000;
        }

        public void run() {
            FetchItem fit = null;
            while (true) {
                fit = fetchQueues.getFetchItem();
                if (fit == null) {
                    LOG.debug(getName() + " spin-waiting ...");
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
                eventCounter.scope("active threads").incrBy(1);

                LOG.info(getName() + " => activeThreads=" + activeThreads
                        + ", spinWaiting=" + spinWaiting + ", queueID="
                        + fit.queueID);

                try {

                    Protocol protocol = protocolFactory.getProtocol(new URL(
                            fit.url));

                    ProtocolResponse response = protocol
                            .getProtocolOutput(fit.url);

                    LOG.info("Fetched " + fit.url + " with status "
                            + response.getStatusCode());

                    eventCounter.scope("fetched").incrBy(1);

                    response.getMetadata().put(
                            "fetch.statusCode",
                            new String[] { Integer.toString(response
                                    .getStatusCode()) });

                    // update the stats
                    // eventStats.scope("KB downloaded").update((long)
                    // content.length / 1024l);
                    // eventStats.scope("# pages").update(1);

                    if (fit.t.contains("metadata")) {
                        HashMap<String, String[]> metadata = (HashMap<String, String[]>) fit.t
                                .getValueByField("metadata");

                        if (metadata != null && !metadata.isEmpty()) {
                            for (Entry<String, String[]> entry : metadata
                                    .entrySet())
                                response.metadata.put(entry.getKey(),
                                        entry.getValue());
                        }
                    }
                    emitQueue.add(new Object[] {
                            Utils.DEFAULT_STREAM_ID,
                            fit.t,
                            new Values(fit.url, response.content,
                                    response.metadata) });
                    synchronized (ackQueue) {
                        ackQueue.add(fit.t);
                    }
                } catch (java.util.concurrent.ExecutionException exece) {
                    if (exece.getCause() instanceof java.util.concurrent.TimeoutException)
                        LOG.error("Socket timeout fetching " + fit.url);
                    else
                        LOG.error("Exception while fetching " + fit.url, exece);
                    synchronized (failQueue) {
                        failQueue.add(fit.t);
                    }
                } catch (Exception e) {
                    LOG.error("Exception while fetching " + fit.url, e);
                    synchronized (failQueue) {
                        failQueue.add(fit.t);
                    }
                } finally {
                    if (fit != null)
                        fetchQueues.finishFetchItem(fit);
                    activeThreads.decrementAndGet(); // count threads
                    eventCounter.scope("active threads").incrBy(-1);
                }
            }

        }
    }

    private void checkConfiguration() {

        // ensure that a value has been set for the agent name and that that
        // agent name is the first value in the agents we advertise for robot
        // rules parsing
        String agentName = getConf().get("http.agent.name");
        if (agentName == null || agentName.trim().length() == 0) {
            String message = "Fetcher: No agents listed in 'http.agent.name'"
                    + " property.";
            if (LOG.isErrorEnabled()) {
                LOG.error(message);
            }
            throw new IllegalArgumentException(message);
        }
    }

    private Configuration getConf() {
        return this.conf;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {

        _collector = collector;

        Config config = new Config();
        config.putAll(stormConf);
        
        this.conf = StormConfiguration.create();
        int threadCount = getConf().getInt("fetcher.threads.fetch", 10);

        checkConfiguration();

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long start = System.currentTimeMillis();
        if (LOG.isInfoEnabled()) {
            LOG.info("Fetcher: starting at " + sdf.format(start));
        }

        // Register a "MultiCountMetric" to count different events in this bolt
        // Storm will emit the counts every n seconds to a special bolt via a
        // system stream
        // The data can be accessed by registering a "MetricConsumer" in the
        // topology
        this.eventCounter = context.registerMetric("fetcher_counter",
                new MultiCountMetric(), 10);

        this.metricGauge = context.registerMetric("fetcher",
                new MultiCountMetric(), 10);

        protocolFactory = new ProtocolFactory(config);

        this.fetchQueues = new FetchItemQueues(getConf());

        for (int i = 0; i < threadCount; i++) { // spawn threads
            new FetcherThread(getConf()).start();
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url", "content", "metadata"));
    }

    private boolean isTickTuple(Tuple tuple) {
        String sourceComponent = tuple.getSourceComponent();
        String sourceStreamId = tuple.getSourceStreamId();
        return sourceComponent.equals(Constants.SYSTEM_COMPONENT_ID)
                && sourceStreamId.equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 1);
        return conf;
    }

    @Override
    public void execute(Tuple input) {

        // main thread in charge of acking and failing
        // see
        // https://github.com/nathanmarz/storm/wiki/Troubleshooting#nullpointerexception-from-deep-inside-storm

        // have a tick tuple to make sure we don't get starved
        synchronized (ackQueue) {
            for (Tuple toack : this.ackQueue) {
                _collector.ack(toack);
            }
            LOG.info("Acked : " + ackQueue.size());
            ackQueue.clear();
        }

        synchronized (failQueue) {
            for (Tuple toack : this.failQueue) {
                _collector.fail(toack);
            }
            LOG.info("Failed : " + failQueue.size());
            failQueue.clear();
        }

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
            LOG.info("Emitted : " + emitQueue.size());
            emitQueue.clear();
        }

        if (isTickTuple(input)) {
            _collector.ack(input);
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

        LOG.info("Threads : " + this.activeThreads.get() + "\tqueues : "
                + this.fetchQueues.queues.size() + "\tin_queues : "
                + this.fetchQueues.inQueues.get());

        String url = input.getStringByField("url");
        // check whether this tuple has a url field
        if (url == null) {
            LOG.info("Missing url field for tuple " + input);
            // ignore silently
            _collector.ack(input);
            return;
        }

        fetchQueues.addFetchItem(input);
    }

}
