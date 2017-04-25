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
import java.util.LinkedList;
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
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.collapse.CollapseBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.util.ConfUtils;

/**
 * Spout which pulls URL from an ES index. Use a single instance unless you use
 * 'es.status.routing' with the StatusUpdaterBolt, in which case you need to
 * have exactly the same number of spout instances as ES shards. Collapses
 * results to implement politeness and ensure a good diversity of sources.
 **/
public class CollapsingSpout extends AbstractSpout implements
        ActionListener<SearchResponse> {

    private static final Logger LOG = LoggerFactory
            .getLogger(CollapsingSpout.class);

    /** Used to avoid deep paging **/
    private static final String ESMaxStartOffsetParamName = "es.status.max.start.offset";

    private int lastStartOffset = 0;
    private int maxStartOffset = -1;

    @Override
    public void open(Map stormConf, TopologyContext context,
            SpoutOutputCollector collector) {
        maxStartOffset = ConfUtils.getInt(stormConf, ESMaxStartOffsetParamName,
                -1);
        super.open(stormConf, context, collector);
    }

    @Override
    protected void populateBuffer() {
        // not used yet or returned empty results
        if (lastDate == null) {
            lastDate = new Date();
            lastStartOffset = 0;
        }
        // been running same query for too long and paging deep?
        else if (maxStartOffset != -1 && lastStartOffset > maxStartOffset) {
            LOG.info("Reached max start offset {}", lastStartOffset);
            lastStartOffset = 0;
        }

        String formattedLastDate = String.format(DATEFORMAT, lastDate);

        LOG.info("{} Populating buffer with nextFetchDate <= {}", logIdprefix,
                formattedLastDate);

        QueryBuilder queryBuilder = QueryBuilders.rangeQuery("nextFetchDate")
                .lte(formattedLastDate);

        SearchRequestBuilder srb = client.prepareSearch(indexName)
                .setTypes(docType).setSearchType(SearchType.QUERY_THEN_FETCH)
                .setQuery(queryBuilder).setFrom(lastStartOffset)
                .setSize(maxBucketNum).setExplain(false);

        // https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-preference.html
        // _shards:2,3
        if (shardID != -1) {
            srb.setPreference("_shards:" + shardID);
        }

        if (StringUtils.isNotBlank(totalSortField)) {
            FieldSortBuilder sorter = SortBuilders.fieldSort(totalSortField)
                    .order(SortOrder.ASC);
            srb.addSort(sorter);
        }

        CollapseBuilder collapse = new CollapseBuilder(partitionField);
        srb.setCollapse(collapse);

        // group expansion -> sends sub queries for each bucket
        if (maxURLsPerBucket > 1) {
            InnerHitBuilder ihb = new InnerHitBuilder();
            ihb.setSize(maxURLsPerBucket);
            ihb.setName("urls_per_bucket");
            // sort within a bucket
            if (StringUtils.isNotBlank(bucketSortField)) {
                List<SortBuilder<?>> sorts = new LinkedList<>();
                FieldSortBuilder bucketsorter = SortBuilders.fieldSort(
                        bucketSortField).order(SortOrder.ASC);
                sorts.add(bucketsorter);
                ihb.setSorts(sorts);
            }
            collapse.setInnerHits(ihb);
        }

        // dump query to log
        LOG.debug("{} ES query {}", logIdprefix, srb.toString());

        timeStartESQuery = System.currentTimeMillis();
        isInESQuery.set(true);
        srb.execute(this);
    }

    @Override
    public void onFailure(Exception e) {
        LOG.error("{} Exception with ES query", logIdprefix, e);
        isInESQuery.set(false);
    }

    @Override
    public void onResponse(SearchResponse response) {
        long timeTaken = System.currentTimeMillis() - timeStartESQuery;

        SearchHit[] hits = response.getHits().getHits();
        int numBuckets = hits.length;

        // no more results?
        if (numBuckets == 0) {
            lastDate = null;
            lastStartOffset = 0;
        }
        // still got some results but paging won't help
        else if (numBuckets < maxBucketNum) {
            lastStartOffset = 0;
        } else {
            lastStartOffset += numBuckets;
        }

        int alreadyprocessed = 0;
        int numDocs = 0;

        synchronized (buffer) {
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
                for (SearchHit subHit : inMyBucket.hits()) {
                    numDocs++;
                    if (!addHitToBuffer(subHit)) {
                        alreadyprocessed++;
                    }
                }
            }

            // Shuffle the URLs so that we don't get blocks of URLs from the
            // same host or domain
            if (numBuckets != numDocs) {
                Collections.shuffle((List) buffer);
            }
        }

        esQueryTimes.addMeasurement(timeTaken);
        // could be derived from the count of query times above
        eventCounter.scope("ES_queries").incrBy(1);
        eventCounter.scope("ES_docs").incrBy(numDocs);
        eventCounter.scope("already_being_processed").incrBy(alreadyprocessed);

        LOG.info(
                "{} ES query returned {} hits from {} buckets in {} msec with {} already being processed",
                logIdprefix, numDocs, numBuckets, timeTaken, alreadyprocessed);

        // remove lock
        isInESQuery.set(false);
    }

    private final boolean addHitToBuffer(SearchHit hit) {
        Map<String, Object> keyValues = hit.sourceAsMap();
        String url = (String) keyValues.get("url");
        // is already being processed - skip it!
        if (beingProcessed.containsKey(url)) {
            return false;
        }
        Metadata metadata = fromKeyValues(keyValues);
        return buffer.add(new Values(url, metadata));
    }

}
