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
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Values;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.sampler.DiversifiedAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsAggregationBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.util.ConfUtils;

/**
 * Spout which pulls URL from an ES index. Use a single instance unless you use
 * 'es.status.routing' with the StatusUpdaterBolt, in which case you need to
 * have exactly the same number of spout instances as ES shards. Guarantees a
 * good mix of URLs by aggregating them by an arbitrary field e.g.
 * metadata.hostname.
 **/
@SuppressWarnings("serial")
public class AggregationSpout extends AbstractSpout implements
        ActionListener<SearchResponse> {

    private static final Logger LOG = LoggerFactory
            .getLogger(AggregationSpout.class);

    private static final String ESStatusSampleParamName = "es.status.sample";

    private boolean sample = false;

    private String lastDate;

    @Override
    public void open(Map stormConf, TopologyContext context,
            SpoutOutputCollector collector) {
        sample = ConfUtils
                .getBoolean(stormConf, ESStatusSampleParamName, false);
        super.open(stormConf, context, collector);
    }

    @Override
    protected void populateBuffer() {

        if (lastDate == null) {
            lastDate = String.format(DATEFORMAT, new Date());
        }

        LOG.info("{} Populating buffer with nextFetchDate <= {}", logIdprefix,
                lastDate);

        QueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(
                "nextFetchDate").lte(lastDate);

        SearchRequestBuilder srb = client.prepareSearch(indexName)
                .setTypes(docType).setSearchType(SearchType.QUERY_THEN_FETCH)
                .setQuery(rangeQueryBuilder).setFrom(0).setSize(0)
                .setExplain(false);

        TermsAggregationBuilder aggregations = AggregationBuilders
                .terms("partition").field(partitionField).size(maxBucketNum);

        TopHitsAggregationBuilder tophits = AggregationBuilders.topHits("docs")
                .size(maxURLsPerBucket).explain(false);
        // sort within a bucket
        if (StringUtils.isNotBlank(bucketSortField)) {
            FieldSortBuilder sorter = SortBuilders.fieldSort(bucketSortField)
                    .order(SortOrder.ASC);
            tophits.sort(sorter);
        }

        aggregations.subAggregation(tophits);

        // sort between buckets
        if (StringUtils.isNotBlank(totalSortField)) {
            MinAggregationBuilder minBuilder = AggregationBuilders.min(
                    "top_hit").field(totalSortField);
            aggregations.subAggregation(minBuilder);
            aggregations.order(Terms.Order.aggregation("top_hit", true));
        }

        if (sample) {
            DiversifiedAggregationBuilder sab = new DiversifiedAggregationBuilder(
                    "sample");
            sab.field(partitionField).maxDocsPerValue(maxURLsPerBucket);
            sab.shardSize(maxURLsPerBucket * maxBucketNum);
            sab.subAggregation(aggregations);
            srb.addAggregation(sab);
        } else {
            srb.addAggregation(aggregations);
        }

        // https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-preference.html
        // _shards:2,3
        if (shardID != -1) {
            srb.setPreference("_shards:" + shardID);
        }

        // dump query to log
        LOG.debug("{} ES query {}", logIdprefix, srb.toString());

        timeStartESQuery = System.currentTimeMillis();
        isInESQuery.set(true);
        srb.execute(this);
    }

    @Override
    public void onFailure(Exception arg0) {
        LOG.error("Exception with ES query", arg0);
        isInESQuery.set(false);
    }

    @Override
    public void onResponse(SearchResponse response) {
        long timeTaken = System.currentTimeMillis() - timeStartESQuery;

        Aggregations aggregs = response.getAggregations();

        SingleBucketAggregation sample = aggregs.get("sample");
        if (sample != null) {
            aggregs = sample.getAggregations();
        }

        Terms agg = aggregs.get("partition");

        int numhits = 0;
        int numBuckets = 0;
        int alreadyprocessed = 0;

        synchronized (buffer) {
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
                    if (beingProcessed.containsKey(url)) {
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

            // Shuffle the URLs so that we don't get blocks of URLs from the
            // same
            // host or domain
            Collections.shuffle((List) buffer);
        }

        LOG.info(
                "{} ES query returned {} hits from {} buckets in {} msec with {} already being processed",
                logIdprefix, numhits, numBuckets, timeTaken, alreadyprocessed);

        esQueryTimes.addMeasurement(timeTaken);
        eventCounter.scope("already_being_processed").incrBy(alreadyprocessed);
        eventCounter.scope("ES_queries").incrBy(1);
        eventCounter.scope("ES_docs").incrBy(numhits);

        // change the date only if we don't get any results at all
        if (numBuckets == 0) {
            lastDate = null;
        }

        // remove lock
        isInESQuery.set(false);
    }

}
