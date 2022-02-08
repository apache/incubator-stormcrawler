/**
 * Licensed to DigitalPebble Ltd under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership.
 * DigitalPebble licenses this file to You under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.digitalpebble.stormcrawler.elasticsearch.persistence;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;

import com.digitalpebble.stormcrawler.util.ConfUtils;
import java.time.Instant;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.collapse.CollapseBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spout which pulls URL from an ES index. Use a single instance unless you use 'es.status.routing'
 * with the StatusUpdaterBolt, in which case you need to have exactly the same number of spout
 * instances as ES shards. Collapses results to implement politeness and ensure a good diversity of
 * sources.
 */
public class CollapsingSpout extends AbstractSpout implements ActionListener<SearchResponse> {

    private static final Logger LOG = LoggerFactory.getLogger(CollapsingSpout.class);

    /** Used to avoid deep paging * */
    private static final String ESMaxStartOffsetParamName = "es.status.max.start.offset";

    private int lastStartOffset = 0;
    private int maxStartOffset = -1;

    @Override
    public void open(
            Map<String, Object> stormConf,
            TopologyContext context,
            SpoutOutputCollector collector) {
        maxStartOffset = ConfUtils.getInt(stormConf, ESMaxStartOffsetParamName, -1);
        super.open(stormConf, context, collector);
    }

    @Override
    protected void populateBuffer() {
        // not used yet or returned empty results
        if (queryDate == null) {
            queryDate = new Date();
            lastTimeResetToNOW = Instant.now();
            lastStartOffset = 0;
        }
        // been running same query for too long and paging deep?
        else if (maxStartOffset != -1 && lastStartOffset > maxStartOffset) {
            LOG.info("Reached max start offset {}", lastStartOffset);
            lastStartOffset = 0;
        }

        String formattedLastDate = ISODateTimeFormat.dateTimeNoMillis().print(queryDate.getTime());

        LOG.info("{} Populating buffer with nextFetchDate <= {}", logIdprefix, formattedLastDate);

        BoolQueryBuilder queryBuilder =
                boolQuery()
                        .filter(QueryBuilders.rangeQuery("nextFetchDate").lte(formattedLastDate));

        if (filterQueries != null) {
            for (String filterQuery : filterQueries) {
                queryBuilder.filter(QueryBuilders.queryStringQuery(filterQuery));
            }
        }

        SearchRequest request = new SearchRequest(indexName);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(queryBuilder);
        sourceBuilder.from(lastStartOffset);
        sourceBuilder.size(maxBucketNum);
        sourceBuilder.explain(false);
        sourceBuilder.trackTotalHits(false);

        // https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-preference.html
        // _shards:2,3
        // specific shard but ideally a local copy of it
        if (shardID != -1) {
            request.preference("_shards:" + shardID + "|_local");
        }

        if (queryTimeout != -1) {
            sourceBuilder.timeout(new TimeValue(queryTimeout, TimeUnit.SECONDS));
        }

        if (StringUtils.isNotBlank(totalSortField)) {
            sourceBuilder.sort(new FieldSortBuilder(totalSortField).order(SortOrder.ASC));
        }

        CollapseBuilder collapse = new CollapseBuilder(partitionField);

        // group expansion -> sends sub queries for each bucket
        if (maxURLsPerBucket > 1) {
            InnerHitBuilder ihb = new InnerHitBuilder();
            ihb.setSize(maxURLsPerBucket);
            ihb.setName("urls_per_bucket");
            List<SortBuilder<?>> sorts = new LinkedList<>();
            // sort within a bucket
            for (String bsf : bucketSortField) {
                FieldSortBuilder bucketsorter = SortBuilders.fieldSort(bsf).order(SortOrder.ASC);
                sorts.add(bucketsorter);
            }
            if (!sorts.isEmpty()) {
                ihb.setSorts(sorts);
            }
            collapse.setInnerHits(ihb);
        }

        sourceBuilder.collapse(collapse);

        request.source(sourceBuilder);

        // dump query to log
        LOG.debug("{} ES query {}", logIdprefix, request.toString());

        isInQuery.set(true);
        client.searchAsync(request, RequestOptions.DEFAULT, this);
    }

    @Override
    public void onFailure(Exception e) {
        LOG.error("{} Exception with ES query", logIdprefix, e);
        markQueryReceivedNow();
    }

    @Override
    public void onResponse(SearchResponse response) {
        long timeTaken = System.currentTimeMillis() - getTimeLastQuerySent();

        SearchHit[] hits = response.getHits().getHits();
        int numBuckets = hits.length;

        int alreadyprocessed = 0;
        int numDocs = 0;

        for (SearchHit hit : hits) {
            Map<String, SearchHits> innerHits = hit.getInnerHits();
            // wanted just one per bucket : no inner hits
            if (innerHits == null) {
                numDocs++;
                if (!addHitToBuffer(hit)) {
                    alreadyprocessed++;
                }
                continue;
            }
            // more than one per bucket
            SearchHits inMyBucket = innerHits.get("urls_per_bucket");
            for (SearchHit subHit : inMyBucket.getHits()) {
                numDocs++;
                if (!addHitToBuffer(subHit)) {
                    alreadyprocessed++;
                }
            }
        }

        queryTimes.addMeasurement(timeTaken);
        // could be derived from the count of query times above
        eventCounter.scope("ES_queries").incrBy(1);
        eventCounter.scope("ES_docs").incrBy(numDocs);
        eventCounter.scope("already_being_processed").incrBy(alreadyprocessed);

        LOG.info(
                "{} ES query returned {} hits from {} buckets in {} msec with {} already being processed.Took {} msec per doc on average.",
                logIdprefix,
                numDocs,
                numBuckets,
                timeTaken,
                alreadyprocessed,
                ((float) timeTaken / numDocs));

        // reset the value for next fetch date if the previous one is too old
        if (resetFetchDateAfterNSecs != -1) {
            Instant changeNeededOn =
                    Instant.ofEpochMilli(
                            lastTimeResetToNOW.toEpochMilli() + (resetFetchDateAfterNSecs * 1000));
            if (Instant.now().isAfter(changeNeededOn)) {
                LOG.info(
                        "queryDate reset based on resetFetchDateAfterNSecs {}",
                        resetFetchDateAfterNSecs);
                queryDate = null;
                lastStartOffset = 0;
            }
        }

        // no more results?
        if (numBuckets == 0) {
            queryDate = null;
            lastStartOffset = 0;
        }
        // still got some results but paging won't help
        else if (numBuckets < maxBucketNum) {
            lastStartOffset = 0;
        } else {
            lastStartOffset += numBuckets;
        }

        // remove lock
        markQueryReceivedNow();
    }
}
