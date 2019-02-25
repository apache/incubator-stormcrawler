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

package com.digitalpebble.stormcrawler.persistence;

import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import com.digitalpebble.stormcrawler.util.CollectionMetric;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * Common features of spouts which query a backend to generate tuples. Tracks
 * the URLs being processes, with an optional delay before they are removed from
 * the cache. Throttles the rate a which queries are emitted and provides a
 * buffer to store the URLs waiting to be sent.
 * 
 * @since 1.11
 **/

public abstract class AbstractQueryingSpout extends BaseRichSpout {

    /**
     * Time in seconds for which acked or failed URLs will be considered for
     * fetching again, default 30 secs.
     **/
    protected static final String StatusTTLPurgatory = "spout.ttl.purgatory";
    /**
     * Min time to allow between 2 successive queries to the backend. Value in
     * msecs, default 2000.
     **/
    protected static final String StatusMinDelayParamName = "spout.min.delay.queries";
    protected long minDelayBetweenQueries = 2000;

    /**
     * Max time to allow between 2 successive queries to the backend. Value in
     * msecs, default 20000.
     **/
    protected static final String StatusMaxDelayParamName = "spout.max.delay.queries";
    protected long maxDelayBetweenQueries = 20000;

    /**
     * Delay in seconds after which the nextFetchDate filter is set to the
     * current time, default 120. Is used to prevent the search to be limited to
     * a handful of sources.
     **/
    protected static final String resetFetchDateParamName = "spout.reset.fetchdate.after";
    protected int resetFetchDateAfterNSecs = 120;

    protected Instant lastTimeResetToNOW;

    private long timeLastQuerySent = 0;
    private long timeLastQueryReceived = 0;

    private long timestampEmptyBuffer = -1;

    protected MultiCountMetric eventCounter;

    protected Queue<Values> buffer = new LinkedList<>();
    protected HashSet<String> in_buffer = new HashSet<>();

    private SpoutOutputCollector _collector;

    /** Required for implementations doing asynchronous calls **/
    protected AtomicBoolean isInQuery = new AtomicBoolean(false);

    protected CollectionMetric queryTimes;

    @Override
    public void open(Map stormConf, TopologyContext context,
            SpoutOutputCollector collector) {

        int ttlPurgatory = ConfUtils.getInt(stormConf, StatusTTLPurgatory, 30);

        minDelayBetweenQueries = ConfUtils.getLong(stormConf,
                StatusMinDelayParamName, 2000);

        maxDelayBetweenQueries = ConfUtils.getLong(stormConf,
                StatusMaxDelayParamName, maxDelayBetweenQueries);

        beingProcessed = new InProcessMap<>(ttlPurgatory, TimeUnit.SECONDS);

        eventCounter = context.registerMetric("counters",
                new MultiCountMetric(), 10);

        context.registerGauge("buffer_size", () -> buffer.size());
        context.registerGauge("beingProcessed", () -> beingProcessed.size());
        context.registerGauge("inPurgatory", () -> beingProcessed.inCache());

        queryTimes = new CollectionMetric();
        context.registerMetric("spout_query_time_msec", queryTimes, 10);

        resetFetchDateAfterNSecs = ConfUtils.getInt(stormConf,
                resetFetchDateParamName, resetFetchDateAfterNSecs);

        _collector = collector;
    }

    /**
     * Method where specific implementations query the storage. Implementations
     * should call markQueryReceivedNow when the documents have been received.
     **/
    protected abstract void populateBuffer();

    /**
     * Map to keep in-process URLs, ev. with additional information for URL /
     * politeness bucket (hostname / domain etc.). The entries are kept in a
     * cache for a configurable amount of time to avoid that some items are
     * fetched a second time if new items are queried shortly after they have
     * been acked.
     */
    protected InProcessMap<String, String> beingProcessed;
    private boolean active;

    /** Map which holds elements some additional time after the removal. */
    public class InProcessMap<K, V> extends HashMap<K, V> {

        private Cache<K, Optional<V>> deletionCache;

        public InProcessMap(long maxDuration, TimeUnit timeUnit) {
            deletionCache = CacheBuilder.newBuilder()
                    .expireAfterWrite(maxDuration, timeUnit).build();
        }

        @Override
        public boolean containsKey(Object key) {
            boolean incache = super.containsKey(key);
            if (!incache) {
                incache = (deletionCache.getIfPresent(key) != null);
            }
            return incache;
        }

        @Override
        public V remove(Object key) {
            deletionCache.put((K) key, Optional.absent());
            return super.remove(key);
        }

        public long inCache() {
            return deletionCache.size();
        }
    }

    @Override
    public void nextTuple() {
        if (!active)
            return;

        // synchronize access to buffer needed in case of asynchronous
        // queries to the backend
        synchronized (buffer) {

            // force the refresh of the buffer even if the buffer is not empty
            if (!isInQuery.get() && triggerQueries()) {
                populateBuffer();
            }

            if (!buffer.isEmpty()) {
                // track how long the buffer had been empty for
                if (timestampEmptyBuffer != -1) {
                    eventCounter.scope("empty.buffer").incrBy(
                            System.currentTimeMillis() - timestampEmptyBuffer);
                    timestampEmptyBuffer = -1;
                }
                List<Object> fields = buffer.remove();
                String url = fields.get(0).toString();
                this._collector.emit(fields, url);
                beingProcessed.put(url, null);
                in_buffer.remove(url);
                eventCounter.scope("emitted").incrBy(1);
                return;
            } else if (timestampEmptyBuffer == -1) {
                timestampEmptyBuffer = System.currentTimeMillis();
            }
        }

        if (isInQuery.get() || throttleQueries() > 0) {
            // sleep for a bit but not too much in order to give ack/fail a
            // chance
            Utils.sleep(10);
            return;
        }

        // re-populate the buffer
        populateBuffer();

        timeLastQuerySent = System.currentTimeMillis();
    }

    /**
     * Returns the amount of time to wait if the backend was queried too
     * recently and needs throttling or -1 if the backend can be queried
     * straight away.
     **/
    private long throttleQueries() {
        if (timeLastQuerySent != 0) {
            // check that we allowed some time between queries
            long difference = System.currentTimeMillis() - timeLastQuerySent;
            if (difference < minDelayBetweenQueries) {
                return minDelayBetweenQueries - difference;
            }
        }
        return -1;
    }

    /**
     * Indicates whether enough time has elapsed since receiving the results of
     * the previous query so that a new one can be sent even if the buffer is
     * not empty. Applies to asynchronous clients only.
     **/
    private boolean triggerQueries() {
        if (timeLastQueryReceived != 0 && maxDelayBetweenQueries > 0) {
            // check that we allowed some time between queries
            long difference = System.currentTimeMillis()
                    - timeLastQueryReceived;
            if (difference > maxDelayBetweenQueries) {
                return true;
            }
        }
        return false;
    }

    protected long getTimeLastQuerySent() {
        return timeLastQuerySent;
    }

    /**
     * sets the marker that we are in a query to false and timeLastQueryReceived
     * to now
     **/
    protected void markQueryReceivedNow() {
        isInQuery.set(false);
        timeLastQueryReceived = System.currentTimeMillis();
    }

    @Override
    public void activate() {
        active = true;
    }

    @Override
    public void deactivate() {
        active = false;
    }

    @Override
    public void ack(Object msgId) {
        beingProcessed.remove(msgId);
        eventCounter.scope("acked").incrBy(1);
    }

    @Override
    public void fail(Object msgId) {
        beingProcessed.remove(msgId);
        eventCounter.scope("failed").incrBy(1);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url", "metadata"));
    }

}