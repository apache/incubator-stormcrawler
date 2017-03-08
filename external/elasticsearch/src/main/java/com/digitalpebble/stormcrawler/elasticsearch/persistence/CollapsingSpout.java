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
import com.digitalpebble.stormcrawler.util.CollectionMetric;
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

    /** Max duration of Date used for querying. Used to avoid deep paging **/
    private static final String ESMaxSecsSinceQueriedDateParamName = "es.status.max.secs.date";

    private int lastStartOffset = 0;
    private Date lastDate;
    private int maxSecSinceQueriedDate = -1;

    private CollectionMetric esQueryTimes = new CollectionMetric();

    @Override
    public void open(Map stormConf, TopologyContext context,
            SpoutOutputCollector collector) {

        context.registerMetric("ES_query_time_msec", esQueryTimes, 10);

        maxSecSinceQueriedDate = ConfUtils.getInt(stormConf,
                ESMaxSecsSinceQueriedDateParamName, -1);
        super.open(stormConf, context, collector);
    }

    @Override
    protected void populateBuffer() {

        Date now = new Date();
        if (lastDate == null) {
            lastDate = now;
            lastStartOffset = 0;
        }
        // been running same query for too long and paging deep?
        else if (maxSecSinceQueriedDate != -1) {
            Date expired = new Date(lastDate.getTime()
                    + (maxSecSinceQueriedDate * 1000));
            if (expired.before(now)) {
                LOG.info("Last date expired {} now {} - resetting query",
                        expired, now);
                lastDate = now;
                lastStartOffset = 0;
            }
        }

        LOG.info("Populating buffer with nextFetchDate <= {}", lastDate);

        QueryBuilder queryBuilder = QueryBuilders.rangeQuery("nextFetchDate")
                .lte(String.format(DATEFORMAT, lastDate));

        SearchRequestBuilder srb = client.prepareSearch(indexName)
                .setTypes(docType).setSearchType(SearchType.QUERY_THEN_FETCH)
                .setQuery(queryBuilder).setFrom(lastStartOffset)
                .setSize(maxBucketNum).setExplain(false);

        // https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-preference.html
        // _shards:2,3
        if (shardID != -1) {
            srb.setPreference("_shards:" + shardID);
        }

        FieldSortBuilder sorter = SortBuilders.fieldSort(totalSortField).order(
                SortOrder.ASC);
        srb.addSort(sorter);

        CollapseBuilder collapse = new CollapseBuilder(partitionField);
        srb.setCollapse(collapse);

        // group expansion -> sends sub queries for each bucket
        if (maxURLsPerBucket > 1) {
            InnerHitBuilder ihb = new InnerHitBuilder();
            ihb.setSize(maxURLsPerBucket);
            ihb.setName("urls_per_bucket");
            List<SortBuilder<?>> sorts = new LinkedList<>();
            FieldSortBuilder bucketsorter = SortBuilders.fieldSort(
                    bucketSortField).order(SortOrder.ASC);
            sorts.add(bucketsorter);
            ihb.setSorts(sorts);
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
        LOG.error("Exception with ES query", e);
        isInESQuery.set(false);
    }

    @Override
    public void onResponse(SearchResponse response) {

        long end = System.currentTimeMillis();

        SearchHit[] hits = response.getHits().getHits();
        int numBuckets = hits.length;

        // no more results?
        if (numBuckets == 0) {
            lastDate = null;
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
                    addHitToBuffer(hit);
                    continue;
                }
                // more than one per bucket
                SearchHits inMyBucket = innerHits.get("urls_per_bucket");
                for (SearchHit subHit : inMyBucket.hits()) {
                    numDocs++;
                    addHitToBuffer(subHit);
                }
            }

            // Shuffle the URLs so that we don't get blocks of URLs from the
            // same host or domain
            if (numBuckets != numDocs) {
                Collections.shuffle((List) buffer);
            }
        }

        long timeTaken = end - timeStartESQuery;
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

    private final void addHitToBuffer(SearchHit hit) {
        Map<String, Object> keyValues = hit.sourceAsMap();
        String url = (String) keyValues.get("url");
        // is already being processed - skip it!
        if (beingProcessed.containsKey(url)) {
            return;
        }
        Metadata metadata = fromKeyValues(keyValues);
        buffer.add(new Values(url, metadata));
    }

}
