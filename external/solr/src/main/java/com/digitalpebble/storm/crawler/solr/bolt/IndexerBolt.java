/**
 * Licensed to DigitalPebble Ltd under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * DigitalPebble licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.digitalpebble.storm.crawler.solr.bolt;

import java.util.Iterator;
import java.util.Map;

import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.storm.crawler.Metadata;
import com.digitalpebble.storm.crawler.indexing.AbstractIndexerBolt;
import com.digitalpebble.storm.crawler.solr.SolrConnection;
import com.digitalpebble.storm.crawler.util.ConfUtils;

import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class IndexerBolt extends AbstractIndexerBolt {

    private static final Logger LOG = LoggerFactory
            .getLogger(IndexerBolt.class);

    private static final String BOLT_TYPE = "indexer";

    private static final String SolrIndexCollection = "solr.indexer.collection";

    private OutputCollector _collector;

    private String collection;

    private MultiCountMetric eventCounter;

    private SolrConnection connection;

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void prepare(Map conf, TopologyContext context,
            OutputCollector collector) {
        super.prepare(conf, context, collector);

        _collector = collector;

        collection = ConfUtils.getString(conf, IndexerBolt.SolrIndexCollection,
                "collection1");

        try {
            connection = SolrConnection.getConnection(conf, BOLT_TYPE);
        } catch (Exception e) {
            LOG.error("Can't connect to Solr: {}", e);
            throw new RuntimeException(e);
        }

        this.eventCounter = context.registerMetric("SolrIndexerBolt",
                new MultiCountMetric(), 10);
    }

    @Override
    public void cleanup() {
        if (connection != null)
            try {
                connection.close();
            } catch (Exception e) {
                LOG.error("Can't close connection to Solr: {}", e);
            }
    }

    @Override
    public void execute(Tuple tuple) {

        String url = valueForURL(tuple);
        Metadata metadata = (Metadata) tuple.getValueByField("metadata");
        String text = tuple.getStringByField("text");

        boolean keep = filterDocument(metadata);
        if (!keep) {
            eventCounter.scope("Filtered").incrBy(1);
            _collector.ack(tuple);
            return;
        }

        try {
            SolrInputDocument doc = new SolrInputDocument();

            // index text content
            if (fieldNameForText() != null) {
                doc.addField(fieldNameForText(), text);
            }

            // url
            if (fieldNameForURL() != null) {
                doc.addField(fieldNameForURL(), url);
            }

            // select which metadata to index
            Map<String, String[]> keyVals = filterMetadata(metadata);

            Iterator<String> iterator = keyVals.keySet().iterator();
            while (iterator.hasNext()) {
                String fieldName = iterator.next();
                String[] values = keyVals.get(fieldName);
                for (String value : values) {
                    doc.addField(fieldName, value);
                }
            }

            connection.getClient().add(doc);

            _collector.ack(tuple);

            eventCounter.scope("Indexed").incrBy(1);
        } catch (Exception e) {
            LOG.error("Send update request to {} failed due to {}", collection,
                    e);
            _collector.fail(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }
}
