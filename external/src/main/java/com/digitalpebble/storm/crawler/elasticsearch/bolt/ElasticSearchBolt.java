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

import java.io.IOException;
import java.util.Date;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;

import static org.elasticsearch.common.xcontent.XContentFactory.*;
import org.elasticsearch.node.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.digitalpebble.storm.crawler.util.ConfUtils;

/**
 * Sends documents to ElasticSearch. Indexes all the fields from the tuples or a
 * Map <String,Object> from a named field.
 **/

@SuppressWarnings("serial")
public class ElasticSearchBolt extends BaseRichBolt {

    private OutputCollector _collector;
    private Client client;
    private BulkProcessor bulkProcessor;

    private String indexName;
    private String docType;
    private String host;
    private String inputField;
    private boolean generateTimeStamp;

    private MultiCountMetric eventCounter;

    public static final Logger LOG = LoggerFactory
            .getLogger(ElasticSearchBolt.class);

    private static final String ESIndexNameParamName = "es.index.name";
    private static final String ESDocTypeParamName = "es.doc.type";
    private static final String ESHostParamName = "es.hostname";
    private static final String ESInputFieldParamName = "es.input.fieldname";
    private static final String ESGenerateTimeStampParamName = "es.generate.timestamp";

    public void prepare(Map conf, TopologyContext context,
            OutputCollector collector) {
        _collector = collector;

        indexName = ConfUtils.getString(conf,
                ElasticSearchBolt.ESIndexNameParamName, "fetcher");
        docType = ConfUtils.getString(conf,
                ElasticSearchBolt.ESDocTypeParamName, "log");
        host = ConfUtils.getString(conf, ElasticSearchBolt.ESHostParamName,
                "localhost");
        inputField = ConfUtils.getString(conf,
                ElasticSearchBolt.ESInputFieldParamName, null);
        generateTimeStamp = ConfUtils.getBoolean(conf,
                ElasticSearchBolt.ESGenerateTimeStampParamName, false);

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
            bulkProcessor = BulkProcessor
                    .builder(client, new BulkProcessor.Listener() {

                        @Override
                        public void afterBulk(long arg0, BulkRequest arg1,
                                BulkResponse arg2) {
                        }

                        @Override
                        public void afterBulk(long arg0, BulkRequest arg1,
                                Throwable arg2) {
                        }

                        @Override
                        public void beforeBulk(long arg0, BulkRequest arg1) {
                        }

                    }).setFlushInterval(TimeValue.timeValueSeconds(60))
                    .setBulkActions(500).setConcurrentRequests(1).build();

        } catch (Exception e1) {
            LOG.error("Can't connect to ElasticSearch", e1);
            throw new RuntimeException(e1);
        }

        this.eventCounter = context.registerMetric("ElasticSearchIndexer",
                new MultiCountMetric(), 10);
    }

    @Override
    public void cleanup() {
        bulkProcessor.close();
        client.close();
    }

    public void execute(Tuple tuple) {

        try {
            XContentBuilder builder = jsonBuilder().startObject();

            boolean hasTimeStamp = false;

            // field where the map containing the fields is stored
            if (StringUtils.isNotBlank(this.inputField)) {
                Map<String, Object> f = (Map<String, Object>) tuple
                        .getValueByField(this.inputField);

                java.util.Iterator<String> iter = f.keySet().iterator();

                while (iter.hasNext()) {
                    String name = iter.next();
                    Object value = f.get(name);
                    if (name.equalsIgnoreCase("timestamp")) {
                        hasTimeStamp = true;
                    }
                    builder.field(name, value);
                }
            }
            // use all the fields present in the tuple
            else {
                Fields fs = tuple.getFields();
                for (int i = 0; i < fs.size(); i++) {
                    String name = fs.get(i);
                    if (name.equalsIgnoreCase("timestamp")) {
                        hasTimeStamp = true;
                    } else
                        builder.field(name, tuple.getValue(i));
                }
            }

            if (generateTimeStamp && !hasTimeStamp)
                builder.field("timestamp", new Date());

            builder.endObject();

            IndexRequestBuilder request = client.prepareIndex(indexName,
                    docType).setSource(builder);

            bulkProcessor.add(request.request());

            _collector.ack(tuple);

            eventCounter.scope("Indexed").incrBy(1);

        } catch (IOException e) {
            LOG.error("Error sending log tuple to ES", e);
            _collector.fail(tuple);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

}
