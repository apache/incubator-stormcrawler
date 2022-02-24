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

import com.digitalpebble.stormcrawler.Constants;
import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.persistence.AbstractStatusUpdaterBolt;
import com.digitalpebble.stormcrawler.persistence.Status;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads all the documents from a shard and emits them on the status stream. Used for copying an
 * index.
 */
public class ScrollSpout extends AbstractSpout implements ActionListener<SearchResponse> {

    private String scrollId = null;
    private boolean hasFinished = false;

    private Queue<Values> queue = new LinkedList<>();

    private static final Logger LOG = LoggerFactory.getLogger(ScrollSpout.class);

    @Override
    // simplified version of the super method so that we can store the fields in
    // the
    // map of things being processed
    public void nextTuple() {
        synchronized (queue) {
            if (!queue.isEmpty()) {
                List<Object> fields = queue.remove();
                String url = fields.get(0).toString();
                _collector.emit(Constants.StatusStreamName, fields, url);
                beingProcessed.put(url, fields);
                eventCounter.scope("emitted").incrBy(1);
                LOG.debug("{} emitted {}", logIdprefix, url);
                return;
            }
        }

        if (isInQuery.get()) {
            LOG.trace("{} isInquery true", logIdprefix);
            // sleep for a bit but not too much in order to give ack/fail a
            // chance
            Utils.sleep(10);
            return;
        }

        // re-populate the buffer
        populateBuffer();
    }

    @Override
    protected void populateBuffer() {
        if (hasFinished) {
            Utils.sleep(10);
            return;
        }

        // initial request
        if (scrollId == null) {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());
            searchSourceBuilder.size(maxURLsPerBucket * maxBucketNum);
            SearchRequest searchRequest = new SearchRequest(indexName);
            searchRequest.source(searchSourceBuilder);
            searchRequest.scroll(TimeValue.timeValueMinutes(5L));

            // specific shard but ideally a local copy of it
            if (shardID != -1) {
                searchRequest.preference("_shards:" + shardID + "|_local");
            }

            isInQuery.set(true);
            LOG.trace("{} isInquery set to true", logIdprefix);

            client.searchAsync(searchRequest, RequestOptions.DEFAULT, this);

            // dump query to log
            LOG.debug("{} ES query {}", logIdprefix, searchRequest.toString());
            return;
        }

        SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
        scrollRequest.scroll(TimeValue.timeValueMinutes(5L));

        isInQuery.set(true);
        client.scrollAsync(scrollRequest, RequestOptions.DEFAULT, this);
        // dump query to log
        LOG.debug("{} ES query {}", logIdprefix, scrollRequest.toString());
    }

    @Override
    public void onResponse(SearchResponse response) {
        SearchHits hits = response.getHits();
        LOG.info(
                "{} ES query returned {} hits in {} msec",
                logIdprefix,
                hits.getHits().length,
                response.getTook().getMillis());
        hasFinished = hits.getHits().length == 0;
        synchronized (this.queue) {
            // Unlike standard spouts, the scroll queries should never return
            // the same
            // document twice -> no need to look in the buffer or cache
            for (SearchHit hit : hits) {
                Map<String, Object> keyValues = hit.getSourceAsMap();
                String url = (String) keyValues.get("url");
                String status = (String) keyValues.get("status");
                String nextFetchDate = (String) keyValues.get("nextFetchDate");
                Metadata metadata = fromKeyValues(keyValues);
                metadata.setValue(
                        AbstractStatusUpdaterBolt.AS_IS_NEXTFETCHDATE_METADATA, nextFetchDate);
                this.queue.add(new Values(url, metadata, Status.valueOf(status)));
            }
        }
        scrollId = response.getScrollId();
        // remove lock
        markQueryReceivedNow();
    }

    @Override
    public void onFailure(Exception e) {
        LOG.error("{} Exception with ES query", logIdprefix, e);
        markQueryReceivedNow();
    }

    @Override
    public void fail(Object msgId) {
        LOG.info("{}  Fail for {}", logIdprefix, msgId);
        eventCounter.scope("failed").incrBy(1);
        // retrieve the values from being processed and send them back to the
        // queue
        Values v = (Values) beingProcessed.remove(msgId);
        queue.add(v);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(Constants.StatusStreamName, new Fields("url", "metadata", "status"));
    }
}
