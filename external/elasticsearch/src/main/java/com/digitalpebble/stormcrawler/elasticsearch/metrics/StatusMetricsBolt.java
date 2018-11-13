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

package com.digitalpebble.stormcrawler.elasticsearch.metrics;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.TupleUtils;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.MultiSearchResponse.Item;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.elasticsearch.ElasticSearchConnection;
import com.digitalpebble.stormcrawler.persistence.Status;
import com.digitalpebble.stormcrawler.util.ConfUtils;

/**
 * Queries the status index periodically to get the count of URLs per status.
 * This bolt can be connected to the output of any other bolt and will not
 * produce anything as output.
 **/
public class StatusMetricsBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory
            .getLogger(StatusMetricsBolt.class);

    private static final String ESBoltType = "status";
    private static final String ESStatusIndexNameParamName = "es.status.index.name";
    private static final String ESStatusDocTypeParamName = "es.status.doc.type";

    private String indexName;
    private String docType;

    private ElasticSearchConnection connection;

    private Map<String, Long> latestStatusCounts = new HashMap<>(5);

    private int freqStats = 60;

    private OutputCollector _collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {
        _collector = collector;
        indexName = ConfUtils.getString(stormConf, ESStatusIndexNameParamName,
                "status");
        docType = ConfUtils.getString(stormConf, ESStatusDocTypeParamName,
                "doc");
        try {
            connection = ElasticSearchConnection.getConnection(stormConf,
                    ESBoltType);
        } catch (Exception e1) {
            LOG.error("Can't connect to ElasticSearch", e1);
            throw new RuntimeException(e1);
        }

        context.registerMetric("status.count", new IMetric() {
            @Override
            public Object getValueAndReset() {
                return latestStatusCounts;
            }
        }, freqStats);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, freqStats);
        return conf;
    }

    @Override
    public void execute(Tuple input) {
        _collector.ack(input);

        // this bolt can be connected to anything
        // we just want to trigger a new search when the input is a tick tuple
        if (!TupleUtils.isTick(input)) {
            return;
        }

        Status[] slist = new Status[] { Status.DISCOVERED, Status.ERROR,
                Status.FETCH_ERROR, Status.FETCHED, Status.REDIRECTION };

        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();

        // should be faster than running the aggregations
        // sent as a single multisearch
        for (Status s : slist) {
            SearchRequest request = new SearchRequest(indexName).types(docType);
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.query(QueryBuilders.termQuery("status", s.name()));
            sourceBuilder.from(0);
            sourceBuilder.size(0);
            sourceBuilder.explain(false);
            request.source(sourceBuilder);
            multiSearchRequest.add(request);
        }

        long start = System.currentTimeMillis();

        MultiSearchResponse response;
        try {
            response = connection.getClient().multiSearch(multiSearchRequest);
        } catch (IOException e) {
            LOG.error("Exception caught when getting multisearch", e);
            return;
        }

        long end = System.currentTimeMillis();

        LOG.info("Multiquery returned in {} msec", end - start);

        long total = 0l;

        for (int i = 0; i < response.getResponses().length; i++) {
            final Item item = response.getResponses()[i];
            if (item.isFailure()) {
                LOG.warn("failure response when querying for status {}",
                        slist[i].name());
                continue;
            }
            SearchResponse res = item.getResponse();
            long count = res.getHits().getTotalHits();
            latestStatusCounts.put(slist[i].name(), count);
            total += count;
        }

        latestStatusCounts.put("TOTAL", total);
    }

    @Override
    public void cleanup() {
        connection.close();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // NONE - THIS BOLT DOES NOT GET CONNECTED TO ANY OTHERS
    }
}
