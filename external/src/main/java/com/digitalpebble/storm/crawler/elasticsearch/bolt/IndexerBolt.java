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

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.digitalpebble.storm.crawler.Metadata;
import com.digitalpebble.storm.crawler.elasticsearch.ElasticSearchConnection;
import com.digitalpebble.storm.crawler.indexing.AbstractIndexerBolt;
import com.digitalpebble.storm.crawler.util.ConfUtils;

/**
 * Sends documents to ElasticSearch. Indexes all the fields from the tuples or a
 * Map &lt;String,Object&gt; from a named field.
 */
@SuppressWarnings("serial")
public class IndexerBolt extends AbstractIndexerBolt {

    private static final Logger LOG = LoggerFactory
            .getLogger(IndexerBolt.class);

    private static final String ESBoltType = "indexer";

    private static final String ESIndexNameParamName = "es.indexer.index.name";
    private static final String ESDocTypeParamName = "es.indexer.doc.type";
    private static final String ESCreateParamName = "es.indexer.create";

    private OutputCollector _collector;

    private String indexName;
    private String docType;
    private boolean create = false;

    private MultiCountMetric eventCounter;

    private ElasticSearchConnection connection;

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void prepare(Map conf, TopologyContext context,
            OutputCollector collector) {
        super.prepare(conf, context, collector);
        _collector = collector;

        indexName = ConfUtils.getString(conf, IndexerBolt.ESIndexNameParamName,
                "fetcher");
        docType = ConfUtils.getString(conf, IndexerBolt.ESDocTypeParamName,
                "doc");
        create = ConfUtils.getBoolean(conf, IndexerBolt.ESCreateParamName,
                false);

        try {
            connection = ElasticSearchConnection
                    .getConnection(conf, ESBoltType);
        } catch (Exception e1) {
            LOG.error("Can't connect to ElasticSearch", e1);
            throw new RuntimeException(e1);
        }

        this.eventCounter = context.registerMetric("ElasticSearchIndexer",
                new MultiCountMetric(), 10);
    }

    @Override
    public void cleanup() {
        if (connection != null)
            connection.close();
    }

    @Override
    public void execute(Tuple tuple) {

        String url = tuple.getStringByField("url");
        Metadata metadata = (Metadata) tuple.getValueByField("metadata");
        String text = tuple.getStringByField("text");

        boolean keep = filterDocument(metadata);
        if (!keep) {
            eventCounter.scope("Filtered").incrBy(1);
            _collector.ack(tuple);
            return;
        }

        try {
            XContentBuilder builder = jsonBuilder().startObject();

            // display text of the document?
            if (fieldNameForText() != null) {
                builder.field(fieldNameForText(), text);
            }

            // send URL as field?
            if (fieldNameForURL() != null) {
                builder.field(fieldNameForURL(), url);
            }

            // which metadata to display?
            Map<String, String[]> keyVals = filterMetadata(metadata);

            Iterator<String> iterator = keyVals.keySet().iterator();
            while (iterator.hasNext()) {
                String fieldName = iterator.next();
                String[] values = keyVals.get(fieldName);
                for (String value : values) {
                    builder.field(fieldName, value);
                }
            }

            builder.endObject();

            IndexRequestBuilder request = connection.getClient()
                    .prepareIndex(indexName, docType).setSource(builder);

            // set create?
            request.setCreate(create);

            connection.getProcessor().add(request.request());

            _collector.ack(tuple);

            eventCounter.scope("Indexed").incrBy(1);

        } catch (IOException e) {
            LOG.error("Error sending log tuple to ES", e);
            _collector.fail(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

}
