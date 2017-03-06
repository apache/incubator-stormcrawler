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

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Values;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.collapse.CollapseBuilder;
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
 * have exactly the same number of spout instances as ES shards. Collapses
 * results to implement politeness and ensure a good diversity of sources.
 **/
public class ElasticSearchSpout extends AbstractSpout implements
        ActionListener<SearchResponse> {

    private static final Logger LOG = LoggerFactory
            .getLogger(ElasticSearchSpout.class);

    private static final String ESStatusBucketFieldParamName = "es.status.bucket.field";
    private static final String ESStatusBufferSizeParamName = "es.status.max.buffer.size";
    private static final String ESStatusMaxURLsParamName = "es.status.max.urls.per.bucket";
    private static final String ESMaxSecsSinceQueriedDateParamName = "es.status.max.secs.date";
    private static final String ESStatusSortFieldParamName = "es.status.sort.field";

    private int maxBufferSize = 100;

    private int lastStartOffset = 0;
    private Date lastDate;
    private int maxSecSinceQueriedDate = -1;

    // TODO implement this
    private int maxURLsPerBucket = 1;

    // when using multiple instances - each one is in charge of a specific shard
    // useful when sharding based on host or domain to guarantee a good mix of
    // URLs
    private int shardID = -1;

    private String sortField;

    private String partitionField;

    @Override
    public void open(Map stormConf, TopologyContext context,
            SpoutOutputCollector collector) {

        maxURLsPerBucket = ConfUtils.getInt(stormConf,
                ESStatusMaxURLsParamName, 1);
        maxBufferSize = ConfUtils.getInt(stormConf,
                ESStatusBufferSizeParamName, 100);

        maxSecSinceQueriedDate = ConfUtils.getInt(stormConf,
                ESMaxSecsSinceQueriedDateParamName, -1);

        sortField = ConfUtils.getString(stormConf, ESStatusSortFieldParamName,
                "nextFetchDate");

        partitionField = ConfUtils.getString(stormConf,
                ESStatusBucketFieldParamName, "metadata.hostname");

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
                .setSize(maxBufferSize).setExplain(false);

        // https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-preference.html
        // _shards:2,3
        if (shardID != -1) {
            srb.setPreference("_shards:" + shardID);
        }

        FieldSortBuilder sorter = SortBuilders.fieldSort(sortField).order(
                SortOrder.ASC);
        srb.addSort(sorter);

        CollapseBuilder collapse = new CollapseBuilder(partitionField);
        srb.setCollapse(collapse);

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

        eventCounter.scope("ES_query_time_msec").incrBy(end - timeStartESQuery);

        SearchHits hits = response.getHits();
        int numhits = hits.getHits().length;

        LOG.info("ES query returned {} hits in {} msec", numhits, end
                - timeStartESQuery);

        eventCounter.scope("ES_queries").incrBy(1);
        eventCounter.scope("ES_docs").incrBy(numhits);

        // no more results?
        if (numhits == 0) {
            lastDate = null;
            lastStartOffset = 0;
        } else {
            lastStartOffset += numhits;
        }

        int alreadyprocessed = 0;
        int numBuckets = 0;

        synchronized (buffer) {

            // filter results so that we don't include URLs we are already
            // being processed or skip those for which we already have enough
            //
            for (int i = 0; i < hits.getHits().length; i++) {
                Map<String, Object> keyValues = hits.getHits()[i].sourceAsMap();
                String url = (String) keyValues.get("url");
                numBuckets++;
                // is already being processed - skip it!
                if (beingProcessed.containsKey(url)) {
                    alreadyprocessed++;
                    eventCounter.scope("already_being_processed").incrBy(1);
                    continue;
                }

                Metadata metadata = fromKeyValues(keyValues);
                buffer.add(new Values(url, metadata));
            }

            // Shuffle the URLs so that we don't get blocks of URLs from the
            // same
            // host or domain
            Collections.shuffle((List) buffer);
        }

        LOG.info(
                "{} ES query returned {} hits from {} buckets in {} msec with {} already being processed",
                logIdprefix, numhits, numBuckets, end - timeStartESQuery,
                alreadyprocessed);

        // remove lock
        isInESQuery.set(false);
    }

}
