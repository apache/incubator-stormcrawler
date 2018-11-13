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

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Values;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.sampler.DiversifiedAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.joda.time.format.ISODateTimeFormat;
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
    private static final String ESMostRecentDateIncreaseParamName = "es.status.recentDate.increase";
    private static final String ESMostRecentDateMinGapParamName = "es.status.recentDate.min.gap";

    private boolean sample = false;

    private int recentDateIncrease = -1;
    private int recentDateMinGap = -1;

    @Override
    public void open(Map stormConf, TopologyContext context,
            SpoutOutputCollector collector) {
        sample = ConfUtils.getBoolean(stormConf, ESStatusSampleParamName,
                sample);
        recentDateIncrease = ConfUtils.getInt(stormConf,
                ESMostRecentDateIncreaseParamName, recentDateIncrease);
        recentDateMinGap = ConfUtils.getInt(stormConf,
                ESMostRecentDateMinGapParamName, recentDateMinGap);
        super.open(stormConf, context, collector);
    }

    @Override
    protected void populateBuffer() {

        if (queryDate == null) {
            queryDate = new Date();
            lastTimeResetToNOW = Instant.now();
        }

        String formattedQueryDate = ISODateTimeFormat.dateTimeNoMillis().print(
                queryDate.getTime());

        LOG.info("{} Populating buffer with nextFetchDate <= {}", logIdprefix,
                formattedQueryDate);

        QueryBuilder queryBuilder = QueryBuilders.rangeQuery("nextFetchDate")
                .lte(formattedQueryDate);

        if (filterQuery != null) {
            queryBuilder = boolQuery().must(queryBuilder).filter(
                    QueryBuilders.queryStringQuery(filterQuery));
        }

        SearchRequest request = new SearchRequest(indexName).types(docType)
                .searchType(SearchType.QUERY_THEN_FETCH);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(queryBuilder);
        sourceBuilder.from(0);
        sourceBuilder.size(0);
        sourceBuilder.explain(false);
        sourceBuilder.trackTotalHits(false);

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
            aggregations.order(BucketOrder.aggregation("top_hit", true));
        }

        if (sample) {
            DiversifiedAggregationBuilder sab = new DiversifiedAggregationBuilder(
                    "sample");
            sab.field(partitionField).maxDocsPerValue(maxURLsPerBucket);
            sab.shardSize(maxURLsPerBucket * maxBucketNum);
            sab.subAggregation(aggregations);
            sourceBuilder.aggregation(sab);
        } else {
            sourceBuilder.aggregation(aggregations);
        }

        request.source(sourceBuilder);

        // https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-preference.html
        // _shards:2,3
        if (shardID != -1) {
            request.preference("_shards:" + shardID);
        }

        // dump query to log
        LOG.debug("{} ES query {}", logIdprefix, request.toString());

        isInQuery.set(true);
        client.searchAsync(request, this);
    }

    @Override
    public void onFailure(Exception arg0) {
        LOG.error("Exception with ES query", arg0);
        isInQuery.set(false);
    }

    @Override
    public void onResponse(SearchResponse response) {
        long timeTaken = System.currentTimeMillis() - timeLastQuery;

        Aggregations aggregs = response.getAggregations();

        if (aggregs == null) {
            isInQuery.set(false);
            return;
        }

        SingleBucketAggregation sample = aggregs.get("sample");
        if (sample != null) {
            aggregs = sample.getAggregations();
        }

        Terms agg = aggregs.get("partition");

        int numhits = 0;
        int numBuckets = 0;
        int alreadyprocessed = 0;

        Date mostRecentDateFound = null;

        SimpleDateFormat formatter = new SimpleDateFormat(
                "yyyy-MM-dd'T'HH:mm:ss.SSSX");

        synchronized (buffer) {
            // For each entry
            Iterator<Terms.Bucket> iterator = (Iterator<Bucket>) agg
                    .getBuckets().iterator();
            while (iterator.hasNext()) {
                Terms.Bucket entry = iterator.next();
                String key = (String) entry.getKey(); // bucket key
                long docCount = entry.getDocCount(); // Doc count

                int hitsForThisBucket = 0;

                // filter results so that we don't include URLs we are already
                // being processed
                TopHits topHits = entry.getAggregations().get("docs");
                for (SearchHit hit : topHits.getHits().getHits()) {
                    hitsForThisBucket++;

                    Map<String, Object> keyValues = hit.getSourceAsMap();
                    String url = (String) keyValues.get("url");
                    LOG.debug("{} -> id [{}], _source [{}]", logIdprefix,
                            hit.getId(), hit.getSourceAsString());

                    // consider only the first document of the last bucket
                    // for optimising the nextFetchDate
                    if (hitsForThisBucket == 1 && !iterator.hasNext()) {
                        String strDate = (String) keyValues
                                .get("nextFetchDate");
                        try {
                            mostRecentDateFound = formatter.parse(strDate);
                        } catch (ParseException e) {
                            throw new RuntimeException("can't parse date :"
                                    + strDate);
                        }
                    }

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
                "{} ES query returned {} hits from {} buckets in {} msec with {} already being processed. Took {} msec per doc on average.",
                logIdprefix, numhits, numBuckets, timeTaken, alreadyprocessed,
                ((float) timeTaken / numhits));

        queryTimes.addMeasurement(timeTaken);
        eventCounter.scope("already_being_processed").incrBy(alreadyprocessed);
        eventCounter.scope("ES_queries").incrBy(1);
        eventCounter.scope("ES_docs").incrBy(numhits);

        // optimise the nextFetchDate by getting the most recent value
        // returned in the query and add to it, unless the previous value is
        // within n mins in which case we'll keep it
        if (mostRecentDateFound != null && recentDateIncrease >= 0) {
            Calendar potentialNewDate = Calendar.getInstance();
            potentialNewDate.setTime(mostRecentDateFound);
            potentialNewDate.add(Calendar.MINUTE, recentDateIncrease);
            Date oldDate = null;
            // check boundaries
            if (this.recentDateMinGap > 0) {
                Calendar low = Calendar.getInstance();
                low.setTime(queryDate);
                low.add(Calendar.MINUTE, -recentDateMinGap);
                Calendar high = Calendar.getInstance();
                high.setTime(queryDate);
                high.add(Calendar.MINUTE, recentDateMinGap);
                if (high.before(potentialNewDate)
                        || low.after(potentialNewDate)) {
                    oldDate = queryDate;
                }
            } else {
                oldDate = queryDate;
            }
            if (oldDate != null) {
                queryDate = potentialNewDate.getTime();
                LOG.info(
                        "{} queryDate changed from {} to {} based on mostRecentDateFound {}",
                        logIdprefix, oldDate, queryDate, mostRecentDateFound);
            } else {
                LOG.info(
                        "{} queryDate kept at {} based on mostRecentDateFound {}",
                        logIdprefix, queryDate, mostRecentDateFound);
            }
        }

        // reset the value for next fetch date if the previous one is too old
        if (resetFetchDateAfterNSecs != -1) {
            Instant changeNeededOn = Instant.ofEpochMilli(lastTimeResetToNOW
                    .toEpochMilli() + (resetFetchDateAfterNSecs * 1000));
            if (Instant.now().isAfter(changeNeededOn)) {
                LOG.info(
                        "{} queryDate set to null based on resetFetchDateAfterNSecs {}",
                        logIdprefix, resetFetchDateAfterNSecs);
                queryDate = null;
            }
        }

        // change the date if we don't get any results at all
        if (numBuckets == 0) {
            queryDate = null;
        }

        // remove lock
        isInQuery.set(false);
    }

}
