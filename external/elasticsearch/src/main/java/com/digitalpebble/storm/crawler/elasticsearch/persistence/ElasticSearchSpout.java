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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.digitalpebble.storm.crawler.Metadata;
import com.digitalpebble.storm.crawler.elasticsearch.ElasticSearchConnection;
import com.digitalpebble.storm.crawler.util.ConfUtils;
import com.digitalpebble.storm.crawler.util.URLPartitioner;

/**
 * Overly simplistic spout implementation which pulls URL from an ES index.
 * Doesn't do anything about data locality or sharding.
 * **/
public class ElasticSearchSpout extends BaseRichSpout {

    private static final Logger LOG = LoggerFactory
            .getLogger(ElasticSearchSpout.class);

    private static final String ESBoltType = "status";

    private static final String ESStatusIndexNameParamName = "es.status.index.name";
    private static final String ESStatusDocTypeParamName = "es.status.doc.type";
    private static final String ESStatusMaxInflightParamName = "es.status.max.inflight.urls.per.bucket";

    private String indexName;
    private String docType;

    private SpoutOutputCollector _collector;

    private Client client;

    private final int bufferSize = 100;

    private Queue<Values> buffer = new LinkedList<Values>();

    private int lastStartOffset = 0;

    private URLPartitioner partitioner;

    private int maxInFlightURLsPerBucket = -1;

    /** Keeps a count of the URLs being processed per host/domain/IP **/
    private Map<String, Integer> inFlightTracker = new HashMap<String, Integer>();

    // URL / politeness bucket (hostname / domain etc...)
    private Map<String, String> beingProcessed = new HashMap<String, String>();

    @Override
    public void open(Map stormConf, TopologyContext context,
            SpoutOutputCollector collector) {
        indexName = ConfUtils.getString(stormConf, ESStatusIndexNameParamName,
                "status");
        docType = ConfUtils.getString(stormConf, ESStatusDocTypeParamName,
                "status");
        maxInFlightURLsPerBucket = ConfUtils.getInt(stormConf,
                ESStatusMaxInflightParamName, 1);
        try {
            client = ElasticSearchConnection.getClient(stormConf, ESBoltType);
        } catch (Exception e1) {
            LOG.error("Can't connect to ElasticSearch", e1);
            throw new RuntimeException(e1);
        }

        partitioner = new URLPartitioner();
        partitioner.configure(stormConf);

        _collector = collector;
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
        // have anything in the buffer?
        if (!buffer.isEmpty()) {
            Values fields = buffer.remove();
            String url = fields.get(0).toString();
            Metadata metadata = (Metadata) fields.get(1);

            String partitionKey = partitioner.getPartition(url, metadata);

            // check whether we already have too tuples in flight for this
            // partition key

            if (maxInFlightURLsPerBucket != -1) {
                Integer inflightforthiskey = inFlightTracker.get(partitionKey);
                if (inflightforthiskey == null)
                    inflightforthiskey = new Integer(0);
                if (inflightforthiskey.intValue() >= maxInFlightURLsPerBucket) {
                    // do it later! left it out of the queue for now
                    return;
                }
                int currentCount = inflightforthiskey.intValue();
                inFlightTracker.put(partitionKey, ++currentCount);
            }

            beingProcessed.put(url, partitionKey);

            this._collector.emit(fields, url);
            return;
        }
        // re-populate the buffer
        populateBuffer();
    }

    /** run a query on ES to populate the internal buffer **/
    private void populateBuffer() {
        // TODO cap the number of results per shard
        // assuming that the sharding of status URLs is done
        // based on the hostname domain or anything else
        // which is useful for politeness

        // TODO cap the results per host or domain

        Date now = new Date();

        // TODO use scrolls instead?
        // @see
        // http://www.elasticsearch.org/guide/en/elasticsearch/client/java-api/current/search.html#scrolling
        SearchResponse response = client
                .prepareSearch(indexName)
                .setTypes(docType)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.rangeQuery("nextFetchDate").lte(now))
                // .setPostFilter(
                // FilterBuilders.rangeFilter("age").from(12).to(18))
                .setFrom(lastStartOffset).setSize(this.bufferSize)
                .setExplain(false).execute().actionGet();

        SearchHits hits = response.getHits();
        int numhits = hits.getHits().length;

        // no more results?
        if (numhits == 0)
            lastStartOffset = 0;
        else
            lastStartOffset += numhits;

        // filter results so that we don't include URLs we are already
        // being processed or skip those for which we already have enough
        //
        for (int i = 0; i < hits.getHits().length; i++) {
            Map<String, Object> keyValues = hits.getHits()[i].sourceAsMap();
            String url = (String) keyValues.get("url");

            // is already being processed - skip it!
            if (beingProcessed.containsKey(url))
                continue;

            String mdAsString = (String) keyValues.get("metadata");
            Metadata metadata = new Metadata();
            if (mdAsString != null) {
                // parse the string and generate the MD accordingly
                // url.path: http://www.lemonde.fr/
                // depth: 1
                String[] kvs = mdAsString.split("\n");
                for (String pair : kvs) {
                    String[] kv = pair.split(": ");
                    if (kv.length != 2) {
                        LOG.info("Invalid key value pair {}", pair);
                        continue;
                    }
                    metadata.addValue(kv[0], kv[1]);
                }
            }
            buffer.add(new Values(url, metadata));
        }
    }

    @Override
    public void ack(Object msgId) {
        super.ack(msgId);
        String partitionKey = beingProcessed.remove(msgId);
        decrementPartitionKey(partitionKey);
    }

    @Override
    public void fail(Object msgId) {
        super.fail(msgId);
        String partitionKey = beingProcessed.remove(msgId);
        decrementPartitionKey(partitionKey);
    }

    private void decrementPartitionKey(String partitionKey) {
        if (partitionKey == null)
            return;
        Integer currentValue = this.inFlightTracker.get(partitionKey);
        if (currentValue == null)
            return;
        int currentVal = currentValue.intValue();
        currentVal--;
        this.inFlightTracker.put(partitionKey, currentVal);
    }

}
