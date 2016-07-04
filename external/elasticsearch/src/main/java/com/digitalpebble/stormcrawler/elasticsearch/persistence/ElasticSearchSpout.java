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

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.elasticsearch.ElasticSearchConnection;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import com.digitalpebble.stormcrawler.util.URLPartitioner;

import org.apache.storm.metric.api.IMetric;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * Spout which pulls URL from an ES index. Use a single instance unless you use
 * 'es.status.routing' with the StatusUpdaterBolt, in which case you need to
 * have exactly the same number of spout instances as ES shards.
 **/
public class ElasticSearchSpout extends BaseRichSpout {

    private static final Logger LOG = LoggerFactory
            .getLogger(ElasticSearchSpout.class);

    private static final String ESBoltType = "status";

    private static final String ESStatusIndexNameParamName = "es.status.index.name";
    private static final String ESStatusDocTypeParamName = "es.status.doc.type";
    private static final String ESStatusBufferSizeParamName = "es.status.max.buffer.size";
    private static final String ESStatusMaxInflightParamName = "es.status.max.inflight.urls.per.bucket";
    private static final String ESRandomSortParamName = "es.status.random.sort";
    private static final String ESMaxSecsSinceQueriedDateParamName = "es.status.max.secs.date";
    private static final String ESStatusSortFieldParamName = "es.status.sort.field";

    private String indexName;
    private String docType;

    private SpoutOutputCollector _collector;

    private static Client client;

    private int maxBufferSize = 100;

    private Queue<Values> buffer = new LinkedList<>();

    private int lastStartOffset = 0;
    private Date lastDate;
    private int maxSecSinceQueriedDate = -1;

    private URLPartitioner partitioner;

    private int maxInFlightURLsPerBucket = -1;

    // sort results randomly to get better diversity of results
    // otherwise sort by the value of es.status.sort.field
    // (default "nextFetchDate")
    boolean randomSort = true;

    /** Keeps a count of the URLs being processed per host/domain/IP **/
    private Map<String, AtomicInteger> inFlightTracker = new HashMap<>();

    // URL / politeness bucket (hostname / domain etc...)
    private Map<String, String> beingProcessed = new HashMap<>();

    private MultiCountMetric eventCounter;

    private boolean active = true;

    // when using multiple instances - each one is in charge of a specific shard
    // useful when sharding based on host or domain to guarantee a good mix of
    // URLs
    private int shardID = -1;

    private String sortField;

    @Override
    public void open(Map stormConf, TopologyContext context,
            SpoutOutputCollector collector) {

        indexName = ConfUtils.getString(stormConf, ESStatusIndexNameParamName,
                "status");
        docType = ConfUtils.getString(stormConf, ESStatusDocTypeParamName,
                "status");
        maxInFlightURLsPerBucket = ConfUtils.getInt(stormConf,
                ESStatusMaxInflightParamName, 1);
        maxBufferSize = ConfUtils.getInt(stormConf,
                ESStatusBufferSizeParamName, 100);
        randomSort = ConfUtils.getBoolean(stormConf, ESRandomSortParamName,
                true);
        maxSecSinceQueriedDate = ConfUtils.getInt(stormConf,
                ESMaxSecsSinceQueriedDateParamName, -1);

        sortField = ConfUtils.getString(stormConf, ESStatusSortFieldParamName,
                "nextFetchDate");

        // one ES client per JVM
        synchronized (ElasticSearchSpout.class) {
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
            // determine the number of shards so that we can restrict the
            // search
            ClusterSearchShardsRequest request = new ClusterSearchShardsRequest(
                    indexName);
            ClusterSearchShardsResponse shardresponse = client.admin()
                    .cluster().searchShards(request).actionGet();
            ClusterSearchShardsGroup[] shardgroups = shardresponse.getGroups();
            if (totalTasks != shardgroups.length) {
                throw new RuntimeException(
                        "Number of ES spout instances should be the same as number of shards ("
                                + shardgroups.length + ") but is " + totalTasks);
            }
            shardID = shardgroups[context.getThisTaskIndex()].getShardId();
            LOG.info("Assigned shard ID {}", shardID);
        }

        partitioner = new URLPartitioner();
        partitioner.configure(stormConf);

        _collector = collector;

        this.eventCounter = context.registerMetric("counters",
                new MultiCountMetric(), 10);

        context.registerMetric("beingProcessed", new IMetric() {
            @Override
            public Object getValueAndReset() {
                return beingProcessed.size();
            }
        }, 10);

        context.registerMetric("buffer_size", new IMetric() {
            @Override
            public Object getValueAndReset() {
                return buffer.size();
            }
        }, 10);
    }

    @Override
    public void close() {
        if (client != null)
            client.close();
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

        // have anything in the buffer?
        if (!buffer.isEmpty()) {
            Values fields = buffer.remove();

            String url = fields.get(0).toString();
            Metadata metadata = (Metadata) fields.get(1);

            String partitionKey = partitioner.getPartition(url, metadata);

            // check whether we already have too many tuples in flight for this
            // partition key

            if (maxInFlightURLsPerBucket != -1) {
                AtomicInteger inflightforthiskey = inFlightTracker
                        .get(partitionKey);
                if (inflightforthiskey == null) {
                    inflightforthiskey = new AtomicInteger();
                    inFlightTracker.put(partitionKey, inflightforthiskey);
                } else if (inflightforthiskey.intValue() >= maxInFlightURLsPerBucket) {
                    // do it later! left it out of the queue for now
                    LOG.debug(
                            "Reached max in flight allowed ({}) for bucket {}",
                            maxInFlightURLsPerBucket, partitionKey);
                    eventCounter.scope("skipped.max.per.bucket").incrBy(1);
                    return;
                }
                inflightforthiskey.incrementAndGet();
            }

            beingProcessed.put(url, partitionKey);

            this._collector.emit(fields, url);
            eventCounter.scope("emitted").incrBy(1);

            return;
        }
        // re-populate the buffer
        populateBuffer();
    }

    /** run a query on ES to populate the internal buffer **/
    private void populateBuffer() {

        if (lastDate == null) {
            lastDate = new Date();
        }

        LOG.info("Populating buffer with nextFetchDate <= {}", lastDate);

        QueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(
                "nextFetchDate").lte(lastDate);
        QueryBuilder queryBuilder = rangeQueryBuilder;

        if (randomSort) {
            FunctionScoreQueryBuilder fsqb = new FunctionScoreQueryBuilder(
                    rangeQueryBuilder);
            fsqb.add(ScoreFunctionBuilders.randomFunction(lastDate.getTime()));
            queryBuilder = fsqb;
        }

        SearchRequestBuilder srb = client
                .prepareSearch(indexName)
                .setTypes(docType)
                // expensive as it builds global Term/Document Frequencies
                // TODO look for a more appropriate method
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(queryBuilder).setFrom(lastStartOffset)
                .setSize(maxBufferSize).setExplain(false);

        // https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-preference.html
        // _shards:2,3
        if (shardID != -1) {
            srb.setPreference("_shards:" + shardID);
        }

        if (!randomSort) {
            FieldSortBuilder sorter = SortBuilders.fieldSort(sortField).order(
                    SortOrder.ASC);
            srb.addSort(sorter);
        }

        long start = System.currentTimeMillis();
        SearchResponse response = srb.execute().actionGet();
        long end = System.currentTimeMillis();

        eventCounter.scope("ES_query_time_msec").incrBy(end - start);

        SearchHits hits = response.getHits();
        int numhits = hits.getHits().length;

        LOG.info("ES query returned {} hits in {} msec", numhits, end - start);

        eventCounter.scope("ES_queries").incrBy(1);
        eventCounter.scope("ES_docs").incrBy(numhits);

        // no more results?
        if (numhits == 0) {
            lastDate = null;
            lastStartOffset = 0;
        } else {
            lastStartOffset += numhits;
            // been running same query for too long and paging deep?
            if (maxSecSinceQueriedDate != -1) {
                Date now = new Date();
                Date expired = new Date(lastDate.getTime()
                        + (maxSecSinceQueriedDate * 1000));
                if (expired.before(now)) {
                    LOG.info("Last date expired {} now {} - resetting query",
                            expired, now);
                    lastDate = null;
                    lastStartOffset = 0;
                }
            }
        }

        // filter results so that we don't include URLs we are already
        // being processed or skip those for which we already have enough
        //
        for (int i = 0; i < hits.getHits().length; i++) {
            Map<String, Object> keyValues = hits.getHits()[i].sourceAsMap();
            String url = (String) keyValues.get("url");

            // is already being processed - skip it!
            if (beingProcessed.containsKey(url)) {
                eventCounter.scope("already_being_processed").incrBy(1);
                continue;
            }

            Map<String, List<String>> mdAsMap = (Map<String, List<String>>) keyValues
                    .get("metadata");
            Metadata metadata = new Metadata();
            if (mdAsMap != null) {
                Iterator<Entry<String, List<String>>> mdIter = mdAsMap
                        .entrySet().iterator();
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
            buffer.add(new Values(url, metadata));
        }
    }

    @Override
    public void ack(Object msgId) {
        String partitionKey = beingProcessed.remove(msgId);
        decrementPartitionKey(partitionKey);
        eventCounter.scope("acked").incrBy(1);
    }

    @Override
    public void fail(Object msgId) {
        LOG.info("Fail for {}", msgId);
        String partitionKey = beingProcessed.remove(msgId);
        decrementPartitionKey(partitionKey);
        eventCounter.scope("failed").incrBy(1);
    }

    private final void decrementPartitionKey(String partitionKey) {
        if (partitionKey == null)
            return;
        AtomicInteger currentValue = this.inFlightTracker.get(partitionKey);
        if (currentValue == null)
            return;
        int newVal = currentValue.decrementAndGet();
        if (newVal == 0)
            this.inFlightTracker.remove(partitionKey);
    }

    @Override
    public void activate() {
        active = true;
    }

    @Override
    public void deactivate() {
        active = false;
    }

}
