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

package com.digitalpebble.storm.crawler.elasticsearch.persistence;

import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;

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
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.metric.api.IMetric;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.digitalpebble.storm.crawler.Metadata;
import com.digitalpebble.storm.crawler.elasticsearch.ElasticSearchConnection;
import com.digitalpebble.storm.crawler.util.ConfUtils;

/**
 * Spout which pulls URL from an ES index. Use a single instance unless you use
 * 'es.status.routing' with the StatusUpdaterBolt, in which case you need to
 * have exactly the same number of spout instances as ES shards. Guarantees a
 * good mix of URLs by bucketing them by an arbitrary field e.g.
 * metadata.hostname.
 **/
public class ElasticsearchSpout2 extends BaseRichSpout {

    private static final Logger LOG = LoggerFactory
            .getLogger(ElasticsearchSpout2.class);

    private static final String ESBoltType = "status";

    private static final String ESStatusIndexNameParamName = "es.status.index.name";
    private static final String ESStatusDocTypeParamName = "es.status.doc.type";

    private static final String ESStatusRoutingFieldParamName = "es.status.routing.fieldname";

    private static final String ESStatusMaxBucketParamName = "es.status.max.buckets";
    private static final String ESStatusMaxURLsParamName = "es.status.max.urls.per.bucket";

    private String indexName;
    private String docType;

    private SpoutOutputCollector _collector;

    private Client client;

    private Set<String> beingProcessed = new HashSet<String>();

    private Queue<Values> buffer = new LinkedList<Values>();

    /** Field name used for field collapsing e.g. metadata.hostname **/
    private String partitionField;

    private int maxURLsPerBucket = 10;

    private int maxBucketNum = 10;

    private MultiCountMetric eventCounter;

    private boolean active = true;

    // when using multiple instances - each one is in charge of a specific shard
    // useful when sharding based on host or domain to guarantee a good mix of
    // URLs
    private int shardID = -1;

    @Override
    public void open(Map stormConf, TopologyContext context,
            SpoutOutputCollector collector) {

        indexName = ConfUtils.getString(stormConf, ESStatusIndexNameParamName,
                "status");
        docType = ConfUtils.getString(stormConf, ESStatusDocTypeParamName,
                "status");

        partitionField = ConfUtils.getString(stormConf,
                ESStatusRoutingFieldParamName);

        maxURLsPerBucket = ConfUtils.getInt(stormConf,
                ESStatusMaxURLsParamName, 1);
        maxBucketNum = ConfUtils.getInt(stormConf, ESStatusMaxBucketParamName,
                10);
        try {
            client = ElasticSearchConnection.getClient(stormConf, ESBoltType);
        } catch (Exception e1) {
            LOG.error("Can't connect to ElasticSearch", e1);
            throw new RuntimeException(e1);
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
    private void populateBuffer() {

        Date now = new Date();

        LOG.info("Populating buffer with nextFetchDate <= {}", now);

        QueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(
                "nextFetchDate").lte(now);

        SearchRequestBuilder srb = client
                .prepareSearch(indexName)
                .setTypes(docType)
                // expensive as it builds global Term/Document Frequencies
                // TODO look for a more appropriate method
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(rangeQueryBuilder).setFrom(0).setSize(0)
                .setExplain(false);

        TermsBuilder aggregations = AggregationBuilders.terms("partition")
                .field(partitionField).size(maxBucketNum);
        // TODO sort the docs within a bucket by nextFetchDate?
        aggregations.subAggregation(AggregationBuilders.topHits("docs")
                .setSize(maxURLsPerBucket).setExplain(false));
        srb.addAggregation(aggregations);

        // https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-preference.html
        // _shards:2,3
        if (shardID != -1) {
            srb.setPreference("_shards:" + shardID);
        }

        long start = System.currentTimeMillis();
        SearchResponse response = srb.execute().actionGet();
        long end = System.currentTimeMillis();

        eventCounter.scope("ES_query_time_msec").incrBy(end - start);

        Aggregations aggregs = response.getAggregations();

        Terms agg = aggregs.get("partition");

        int numhits = 0;
        int numBuckets = 0;

        // For each entry
        for (Terms.Bucket entry : agg.getBuckets()) {
            String key = entry.getKey(); // bucket key
            long docCount = entry.getDocCount(); // Doc count

            numBuckets++;

            int hitsForThisBucket = 0;

            // filter results so that we don't include URLs we are already
            // being processed
            TopHits topHits = entry.getAggregations().get("docs");
            for (SearchHit hit : topHits.getHits().getHits()) {
                hitsForThisBucket++;

                Map<String, Object> keyValues = hit.sourceAsMap();
                String url = (String) keyValues.get("url");

                LOG.debug(" -> id [{}], _source [{}]", hit.getId(),
                        hit.getSourceAsString());

                // is already being processed - skip it!
                if (beingProcessed.contains(url)) {
                    eventCounter.scope("already_being_processed").incrBy(1);
                    continue;
                }
                Metadata metadata = fromKeyValues(keyValues);
                buffer.add(new Values(url, metadata));
            }

            numhits += hitsForThisBucket;

            LOG.info("key [{}], hits[{}], doc_count [{}]", key,
                    hitsForThisBucket, docCount);
        }

        LOG.info("ES query returned {} hits from {} buckets in {} msec",
                numhits, numBuckets, (end - start));

        eventCounter.scope("ES_queries").incrBy(1);
        eventCounter.scope("ES_docs").incrBy(numhits);
    }

    private final Metadata fromKeyValues(Map<String, Object> keyValues) {
        Map<String, List<String>> mdAsMap = (Map<String, List<String>>) keyValues
                .get("metadata");
        Metadata metadata = new Metadata();
        if (mdAsMap != null) {
            Iterator<Entry<String, List<String>>> mdIter = mdAsMap.entrySet()
                    .iterator();
            while (mdIter.hasNext()) {
                Entry<String, List<String>> mdEntry = mdIter.next();
                String key = mdEntry.getKey();
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
        beingProcessed.remove(msgId);
        eventCounter.scope("acked").incrBy(1);
    }

    @Override
    public void fail(Object msgId) {
        LOG.info("Fail for {}", msgId);
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
