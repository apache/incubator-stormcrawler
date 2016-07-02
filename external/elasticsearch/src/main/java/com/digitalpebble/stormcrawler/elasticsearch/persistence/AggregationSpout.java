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

import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.min.MinBuilder;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.elasticsearch.ElasticSearchConnection;
import com.digitalpebble.stormcrawler.util.ConfUtils;

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
 * have exactly the same number of spout instances as ES shards. Guarantees a
 * good mix of URLs by aggregating them by an arbitrary field e.g.
 * metadata.hostname.
 **/
@SuppressWarnings("serial")
public class AggregationSpout extends BaseRichSpout {

    private static final Logger LOG = LoggerFactory
            .getLogger(AggregationSpout.class);

    private static final String ESBoltType = "status";

    private static final String ESStatusIndexNameParamName = "es.status.index.name";
    private static final String ESStatusDocTypeParamName = "es.status.doc.type";

    private static final String ESStatusRoutingFieldParamName = "es.status.routing.fieldname";

    private static final String ESStatusMaxBucketParamName = "es.status.max.buckets";
    private static final String ESStatusMaxURLsParamName = "es.status.max.urls.per.bucket";

    /**
     * Field name to use for sorting the URLs within a bucket, not used if empty
     * or null.
     **/
    private static final String ESStatusBucketSortFieldParamName = "es.status.bucket.sort.field";

    /**
     * Field name to use for sorting the buckets, not used if empty or null.
     **/
    private static final String ESStatusGlobalSortFieldParamName = "es.status.global.sort.field";

    /**
     * Min time to allow between 2 successive queries to ES. Value in msecs,
     * default 2000.
     **/
    private static final String ESStatusMinDelayParamName = "es.status.min.delay.queries";

    protected String indexName;
    protected String docType;

    private SpoutOutputCollector _collector;

    protected static Client client;

    protected Set<String> beingProcessed = new HashSet<>();

    protected Queue<Values> buffer = new LinkedList<>();

    /** Field name used for field collapsing e.g. metadata.hostname **/
    protected String partitionField;

    protected int maxURLsPerBucket = 10;

    protected int maxBucketNum = 10;

    protected MultiCountMetric eventCounter;

    private boolean active = true;

    protected long minDelayBetweenQueries = 2000;

    /**
     * when using multiple instances - each one is in charge of a specific shard
     * useful when sharding based on host or domain to guarantee a good mix of
     * URLs
     */
    protected int shardID = -1;

    private String bucketSortField = "";

    private String totalSortField = "";

    protected Date timePreviousQuery = null;

    /** Used to distinguish between instances in the logs **/
    protected String logIdprefix = "";

    @Override
    public void open(Map stormConf, TopologyContext context,
            SpoutOutputCollector collector) {

        indexName = ConfUtils.getString(stormConf, ESStatusIndexNameParamName,
                "status");
        docType = ConfUtils.getString(stormConf, ESStatusDocTypeParamName,
                "status");

        partitionField = ConfUtils.getString(stormConf,
                ESStatusRoutingFieldParamName);

        bucketSortField = ConfUtils.getString(stormConf,
                ESStatusBucketSortFieldParamName, bucketSortField);

        totalSortField = ConfUtils.getString(stormConf,
                ESStatusGlobalSortFieldParamName);

        maxURLsPerBucket = ConfUtils.getInt(stormConf,
                ESStatusMaxURLsParamName, 1);
        maxBucketNum = ConfUtils.getInt(stormConf, ESStatusMaxBucketParamName,
                10);

        minDelayBetweenQueries = ConfUtils.getLong(stormConf,
                ESStatusMinDelayParamName, 2000);

        // one ES client per JVM
        synchronized (AggregationSpout.class) {
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
            LOG.info("{} assigned shard ID {}", logIdprefix, shardID);
        }

        _collector = collector;

        this.eventCounter = context.registerMetric("counters",
                new MultiCountMetric(), 10);

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
            beingProcessed.add(url);

            this._collector.emit(fields, url);
            eventCounter.scope("emitted").incrBy(1);

            return;
        }
        // re-populate the buffer
        populateBuffer();
    }

    /** run a query on ES to populate the internal buffer **/
    protected void populateBuffer() {

        Date now = new Date();

        // check that we allowed some time between queries
        if (timePreviousQuery != null) {
            long difference = now.getTime() - timePreviousQuery.getTime();
            if (difference < minDelayBetweenQueries) {
                long sleepTime = minDelayBetweenQueries - difference;
                LOG.info(
                        "{} Not enough time elapsed since {} - sleeping for {}",
                        logIdprefix, timePreviousQuery, sleepTime);
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    LOG.error("{} InterruptedException caught while waiting",
                            logIdprefix);
                }
                return;
            }
        }

        timePreviousQuery = now;

        LOG.info("{} Populating buffer with nextFetchDate <= {}", logIdprefix,
                now);

        QueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(
                "nextFetchDate").lte(now);

        SearchRequestBuilder srb = client.prepareSearch(indexName)
                .setTypes(docType).setSearchType(SearchType.QUERY_THEN_FETCH)
                .setQuery(rangeQueryBuilder).setFrom(0).setSize(0)
                .setExplain(false);

        TermsBuilder aggregations = AggregationBuilders.terms("partition")
                .field("metadata." + partitionField).size(maxBucketNum);

        TopHitsBuilder tophits = AggregationBuilders.topHits("docs")
                .setSize(maxURLsPerBucket).setExplain(false);
        // sort within a bucket
        if (StringUtils.isNotBlank(bucketSortField)) {
            FieldSortBuilder sorter = SortBuilders.fieldSort(bucketSortField)
                    .order(SortOrder.ASC);
            tophits.addSort(sorter);
        }

        aggregations.subAggregation(tophits);

        // sort between buckets
        if (StringUtils.isNotBlank(totalSortField)) {
            MinBuilder minBuilder = AggregationBuilders.min("top_hit").field(
                    totalSortField);
            aggregations.subAggregation(minBuilder);
            aggregations.order(Terms.Order.aggregation("top_hit", true));
        }

        srb.addAggregation(aggregations);

        // https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-preference.html
        // _shards:2,3
        if (shardID != -1) {
            srb.setPreference("_shards:" + shardID);
        }

        // dump query to log
        LOG.debug("{} ES query {}", logIdprefix, srb.toString());

        long start = System.currentTimeMillis();
        SearchResponse response = srb.execute().actionGet();
        long end = System.currentTimeMillis();

        eventCounter.scope("ES_query_time_msec").incrBy(end - start);

        Aggregations aggregs = response.getAggregations();

        Terms agg = aggregs.get("partition");

        int numhits = 0;
        int numBuckets = 0;
        int alreadyprocessed = 0;

        // For each entry
        for (Terms.Bucket entry : agg.getBuckets()) {
            String key = (String) entry.getKey(); // bucket key
            long docCount = entry.getDocCount(); // Doc count

            int hitsForThisBucket = 0;

            // filter results so that we don't include URLs we are already
            // being processed
            TopHits topHits = entry.getAggregations().get("docs");
            for (SearchHit hit : topHits.getHits().getHits()) {
                hitsForThisBucket++;

                Map<String, Object> keyValues = hit.sourceAsMap();
                String url = (String) keyValues.get("url");

                LOG.debug("{} -> id [{}], _source [{}]", logIdprefix,
                        hit.getId(), hit.getSourceAsString());

                // is already being processed - skip it!
                if (beingProcessed.contains(url)) {
                    alreadyprocessed++;
                    continue;
                }
                Metadata metadata = fromKeyValues(keyValues);
                buffer.add(new Values(url, metadata));
            }

            if (hitsForThisBucket > 0)
                numBuckets++;

            numhits += hitsForThisBucket;

            LOG.debug("{} key [{}], hits[{}], doc_count [{}]", logIdprefix,
                    key, hitsForThisBucket, docCount, alreadyprocessed);
        }

        // Shuffle the URLs so that we don't get blocks of URLs from the same
        // host or domain
        Collections.shuffle((List) buffer);

        LOG.info(
                "{} ES query returned {} hits from {} buckets in {} msec with {} already being processed",
                logIdprefix, numhits, numBuckets, end - start, alreadyprocessed);

        eventCounter.scope("already_being_processed").incrBy(alreadyprocessed);
        eventCounter.scope("ES_queries").incrBy(1);
        eventCounter.scope("ES_docs").incrBy(numhits);
    }

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

}
