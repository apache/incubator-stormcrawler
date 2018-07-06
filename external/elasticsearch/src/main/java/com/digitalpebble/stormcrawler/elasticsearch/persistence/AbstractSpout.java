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

package com.digitalpebble.stormcrawler.elasticsearch.persistence;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.storm.metric.api.IMetric;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.elasticsearch.ElasticSearchConnection;
import com.digitalpebble.stormcrawler.util.CollectionMetric;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public abstract class AbstractSpout extends BaseRichSpout {

    private static final Logger LOG = LoggerFactory
            .getLogger(AbstractSpout.class);

    protected static final String ESBoltType = "status";
    protected static final String ESStatusIndexNameParamName = "es.status.index.name";
    protected static final String ESStatusDocTypeParamName = "es.status.doc.type";

    /**
     * Time in seconds for which acked or failed URLs will be considered for
     * fetching again, default 30 secs.
     **/
    protected static final String ESStatusTTLPurgatory = "es.status.ttl.purgatory";

    /** Field name to use for aggregating **/
    protected static final String ESStatusBucketFieldParamName = "es.status.bucket.field";
    protected static final String ESStatusMaxBucketParamName = "es.status.max.buckets";
    protected static final String ESStatusMaxURLsParamName = "es.status.max.urls.per.bucket";

    /**
     * Field name to use for sorting the URLs within a bucket, not used if empty
     * or null.
     **/
    protected static final String ESStatusBucketSortFieldParamName = "es.status.bucket.sort.field";

    /**
     * Field name to use for sorting the buckets, not used if empty or null.
     **/
    protected static final String ESStatusGlobalSortFieldParamName = "es.status.global.sort.field";

    /**
     * Min time to allow between 2 successive queries to ES. Value in msecs,
     * default 2000.
     **/
    private static final String ESStatusMinDelayParamName = "es.status.min.delay.queries";

    protected static final String ESStatusResetFetchDateParamName = "es.status.reset.fetchdate.after";

    protected static final String ESStatusFilterParamName = "es.status.filterQuery";

    protected String filterQuery = null;

    protected String indexName;
    protected String docType;
    protected boolean active = true;
    protected SpoutOutputCollector _collector;
    protected MultiCountMetric eventCounter;

    protected static RestHighLevelClient client;

    /**
     * when using multiple instances - each one is in charge of a specific shard
     * useful when sharding based on host or domain to guarantee a good mix of
     * URLs
     */
    protected int shardID = -1;

    /** Used to distinguish between instances in the logs **/
    protected String logIdprefix = "";

    protected Queue<Values> buffer = new LinkedList<>();

    /**
     * Map to keep in-process URLs, ev. with additional information for URL /
     * politeness bucket (hostname / domain etc.). The entries are kept in a
     * cache for a configurable amount of time to avoid that some items are
     * fetched a second time if new items are queried shortly after they have
     * been acked.
     */
    protected InProcessMap<String, String> beingProcessed;

    protected long timeStartESQuery = 0;

    private long minDelayBetweenQueries = 2000;

    protected AtomicBoolean isInESQuery = new AtomicBoolean(false);

    /** Field name used for field collapsing e.g. metadata.hostname **/
    protected String partitionField;

    protected int maxURLsPerBucket = 10;

    protected int maxBucketNum = 10;

    protected String bucketSortField = "";

    protected String totalSortField = "";

    protected CollectionMetric esQueryTimes;

    protected Date lastDate;

    protected int resetFetchDateAfterNSecs = 120;

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
    public void open(Map stormConf, TopologyContext context,
            SpoutOutputCollector collector) {

        indexName = ConfUtils.getString(stormConf, ESStatusIndexNameParamName,
                "status");
        docType = ConfUtils.getString(stormConf, ESStatusDocTypeParamName,
                "status");

        // one ES client per JVM
        synchronized (AbstractSpout.class) {
            try {
                if (client == null) {
                    client = ElasticSearchConnection.getClient(stormConf,
                            ESBoltType);
                }
            } catch (Exception e1) {
                LOG.error("Can't connect to ElasticSearch", e1);
                throw new RuntimeException(e1);
            }
        }

        // if more than one instance is used we expect their number to be the
        // same as the number of shards
        int totalTasks = context
                .getComponentTasks(context.getThisComponentId()).size();
        if (totalTasks > 1) {
            logIdprefix = "[" + context.getThisComponentId() + " #"
                    + context.getThisTaskIndex() + "] ";

            // determine the number of shards so that we can restrict the
            // search

            // TODO use the admin API when it gets available
            // TODO or the low level one with
            // https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-shards-stores.html
            // TODO identify local shards and use those if possible

            // ClusterSearchShardsRequest request = new
            // ClusterSearchShardsRequest(
            // indexName);
            // ClusterSearchShardsResponse shardresponse = client.admin()
            // .cluster().searchShards(request).actionGet();
            // ClusterSearchShardsGroup[] shardgroups =
            // shardresponse.getGroups();
            // if (totalTasks != shardgroups.length) {
            // throw new RuntimeException(
            // "Number of ES spout instances should be the same as number of
            // shards ("
            // + shardgroups.length + ") but is " + totalTasks);
            // }
            // shardID = shardgroups[context.getThisTaskIndex()].getShardId()
            // .getId();

            // TEMPORARY simply use the task index as shard index
            shardID = context.getThisTaskIndex();
            LOG.info("{} assigned shard ID {}", logIdprefix, shardID);
        }

        _collector = collector;

        int ttlPurgatory = ConfUtils
                .getInt(stormConf, ESStatusTTLPurgatory, 30);

        minDelayBetweenQueries = ConfUtils.getLong(stormConf,
                ESStatusMinDelayParamName, 2000);

        partitionField = ConfUtils.getString(stormConf,
                ESStatusBucketFieldParamName, "metadata.hostname");

        bucketSortField = ConfUtils.getString(stormConf,
                ESStatusBucketSortFieldParamName, bucketSortField);

        totalSortField = ConfUtils.getString(stormConf,
                ESStatusGlobalSortFieldParamName);

        maxURLsPerBucket = ConfUtils.getInt(stormConf,
                ESStatusMaxURLsParamName, 1);
        maxBucketNum = ConfUtils.getInt(stormConf, ESStatusMaxBucketParamName,
                10);

        resetFetchDateAfterNSecs = ConfUtils.getInt(stormConf,
                ESStatusResetFetchDateParamName, resetFetchDateAfterNSecs);

        beingProcessed = new InProcessMap<>(ttlPurgatory, TimeUnit.SECONDS);

        filterQuery = ConfUtils.getString(stormConf, ESStatusFilterParamName);

        eventCounter = context.registerMetric("counters",
                new MultiCountMetric(), 10);

        context.registerMetric("buffer_size", new IMetric() {
            @Override
            public Object getValueAndReset() {
                return buffer.size();
            }
        }, 10);

        context.registerMetric("beingProcessed", new IMetric() {
            @Override
            public Object getValueAndReset() {
                return beingProcessed.size();
            }
        }, 10);

        context.registerMetric("inPurgatory", new IMetric() {
            @Override
            public Object getValueAndReset() {
                return beingProcessed.inCache();
            }
        }, 10);

        esQueryTimes = new CollectionMetric();
        context.registerMetric("ES_query_time_msec", esQueryTimes, 10);
    }

    /** Returns true if ES was queried too recently and needs throttling **/
    protected boolean throttleESQueries() {
        Date now = new Date();
        if (timeStartESQuery != 0) {
            // check that we allowed some time between queries
            long difference = now.getTime() - timeStartESQuery;
            if (difference < minDelayBetweenQueries) {
                long sleepTime = minDelayBetweenQueries - difference;
                LOG.debug(
                        "{} Not enough time elapsed since {} - should try again in {}",
                        logIdprefix, timeStartESQuery, sleepTime);
                return true;
            }
        }
        return false;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url", "metadata"));
    }

    @Override
    public void nextTuple() {

        // inactive?
        if (active == false)
            return;

        synchronized (buffer) {
            // have anything in the buffer?
            if (!buffer.isEmpty()) {
                Values fields = buffer.remove();
                String url = fields.get(0).toString();
                beingProcessed.put(url, null);
                _collector.emit(fields, url);
                eventCounter.scope("emitted").incrBy(1);
                return;
            }
        }

        // check that we allowed some time between queries
        // and not in middle of querying ES
        if (isInESQuery.get() || throttleESQueries()) {
            // sleep for a bit but not too much in order to give ack/fail a
            // chance
            Utils.sleep(10);
            return;
        }

        // re-populate the buffer
        populateBuffer();
    }

    /** Builds a query and use it retrieve the results from ES **/
    protected abstract void populateBuffer();

    protected final Metadata fromKeyValues(Map<String, Object> keyValues) {
        Map<String, List<String>> mdAsMap = (Map<String, List<String>>) keyValues
                .get("metadata");
        Metadata metadata = new Metadata();
        if (mdAsMap != null) {
            Iterator<Entry<String, List<String>>> mdIter = mdAsMap.entrySet()
                    .iterator();
            while (mdIter.hasNext()) {
                Entry<String, List<String>> mdEntry = mdIter.next();
                String key = mdEntry.getKey();
                // periods are not allowed in ES2 - replace with %2E
                key = key.replaceAll("%2E", "\\.");
                Object mdValObj = mdEntry.getValue();
                // single value
                if (mdValObj instanceof String) {
                    metadata.addValue(key, (String) mdValObj);
                }
                // multi valued
                else {
                    metadata.addValues(key, (List<String>) mdValObj);
                }
            }
        }
        return metadata;
    }

    @Override
    public void ack(Object msgId) {
        LOG.debug("{}  Ack for {}", logIdprefix, msgId);
        beingProcessed.remove(msgId);
        eventCounter.scope("acked").incrBy(1);
    }

    @Override
    public void fail(Object msgId) {
        LOG.info("{}  Fail for {}", logIdprefix, msgId);
        beingProcessed.remove(msgId);
        eventCounter.scope("failed").incrBy(1);
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
    public void close() {
        if (client != null)
            try {
                client.close();
            } catch (IOException e) {
            }
    }

}
