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

package com.digitalpebble.storm.crawler.elasticsearch.bolt;

import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
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
import com.digitalpebble.storm.crawler.util.ConfUtils;

/**
 * Overly simplistic spout implementation which pulls URL from an ES index.
 * Doesn't do anything about data locality or sharding.
 * **/
public class ElasticSearchSpout extends BaseRichSpout {

    private static final Logger LOG = LoggerFactory
            .getLogger(ElasticSearchSpout.class);

    private static final String ESStatusIndexNameParamName = "es.status.index.name";
    private static final String ESStatusDocTypeParamName = "es.status.doc.type";
    private static final String ESStatusHostParamName = "es.status.hostname";

    private String indexName;
    private String docType;
    private String host;

    private SpoutOutputCollector _collector;

    private Client client;

    private final int bufferSize = 100;

    private Queue<Values> buffer = new LinkedList<Values>();

    private int lastStartOffset = 0;

    private Set<String> beingProcessed = new HashSet<String>();

    @Override
    public void open(Map stormConf, TopologyContext context,
            SpoutOutputCollector collector) {
        indexName = ConfUtils.getString(stormConf, ESStatusIndexNameParamName,
                "status");
        docType = ConfUtils.getString(stormConf, ESStatusDocTypeParamName,
                "status");
        host = ConfUtils.getString(stormConf, ESStatusHostParamName,
                "localhost");

        // connection to ES
        try {
            if (host.equalsIgnoreCase("localhost")) {
                Node node = org.elasticsearch.node.NodeBuilder.nodeBuilder()
                        .clusterName("elasticsearch").client(true).node();
                client = node.client();
            } else {
                Settings settings = ImmutableSettings.settingsBuilder()
                        .put("cluster.name", "elasticsearch").build();
                client = new TransportClient(settings)
                        .addTransportAddress(new InetSocketTransportAddress(
                                host, 9300));
            }

        } catch (Exception e1) {
            LOG.error("Can't connect to ElasticSearch", e1);
            throw new RuntimeException(e1);
        }

        _collector = collector;
    }

    @Override
    public void close() {
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
            String id = fields.get(0).toString();
            beingProcessed.add(id);
            this._collector.emit(fields, id);
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
        lastStartOffset = hits.getHits().length;

        // TODO filter results so that we don't include URLs we are already
        // processing
        for (int i = 0; i < hits.getHits().length; i++) {
            Map<String, Object> keyValues = hits.getHits()[i].sourceAsMap();
            String url = (String) keyValues.get("url");

            // is already being processed - skip it!
            if (beingProcessed.contains(url))
                continue;

            String mdAsString = (String) keyValues.get("metadata");
            Metadata metadata = new Metadata();
            if (mdAsString != null) {
                // TODO parse the string and generate the MD accordingly
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
        // TODO Auto-generated method stub
        super.ack(msgId);
        beingProcessed.remove(msgId);
    }

    @Override
    public void fail(Object msgId) {
        // TODO Auto-generated method stub
        super.fail(msgId);
        beingProcessed.remove(msgId);
    }

}
