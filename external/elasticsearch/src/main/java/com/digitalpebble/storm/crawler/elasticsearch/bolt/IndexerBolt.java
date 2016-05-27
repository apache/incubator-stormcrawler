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

import static com.digitalpebble.storm.crawler.Constants.StatusStreamName;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterators;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.storm.crawler.Metadata;
import com.digitalpebble.storm.crawler.elasticsearch.ElasticSearchConnection;
import com.digitalpebble.storm.crawler.indexing.AbstractIndexerBolt;
import com.digitalpebble.storm.crawler.persistence.Status;
import com.digitalpebble.storm.crawler.util.ConfUtils;

import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

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

        // Distinguish the value used for indexing
        // from the one used for the status
        String normalisedurl = valueForURL(tuple);

        Metadata metadata = (Metadata) tuple.getValueByField("metadata");
        String text = tuple.getStringByField("text");

        boolean keep = filterDocument(metadata);
        if (!keep) {
            eventCounter.scope("Filtered").incrBy(1);
            // treat it as successfully processed even if
            // we do not index it
            _collector.emit(StatusStreamName, tuple, new Values(url, metadata,
                    Status.FETCHED));
            _collector.ack(tuple);
            return;
        }

        Map<String, Object> json = new HashMap<String, Object>();

        // display text of the document?
        if (fieldNameForText() != null) {
            json.put(fieldNameForText(), text);
        }

        // send URL as field?
        if (fieldNameForURL() != null) {
            json.put(fieldNameForURL(), normalisedurl);
        }

        // which metadata to display?
        Map<String, String[]> filteredMetadata = filterMetadata(metadata);

        Iterator<String> iterator = filteredMetadata.keySet().iterator();
        while (iterator.hasNext()) {
            Map<String, Object> jsonObject = json;
            String fieldName = iterator.next();

            String[] parts = fieldName.split("\\.");
            Iterator<String> it = Iterators.forArray(parts);

            while (it.hasNext()) {
                String part = it.next();
                if (!it.hasNext()) {
                    for (String value : filteredMetadata.get(fieldName)) {
                        jsonObject.put(part, value);
                    }
                } else if (!jsonObject.containsKey(part)) {
                    Map<String, Object> nestedObject = new HashMap<>();
                    jsonObject.put(part, nestedObject);
                    jsonObject = nestedObject;
                } else {
                    Object o = jsonObject.get(part);
                    if (o instanceof Map) {
                        jsonObject = (Map<String, Object>) o;
                    }
                }
            }
        }

        IndexRequestBuilder request = connection.getClient()
                .prepareIndex(indexName, docType).setSource(json)
                .setId(normalisedurl);

        // set create?
        request.setCreate(create);

        connection.getProcessor().add(request.request());

        eventCounter.scope("Indexed").incrBy(1);

        _collector.emit(StatusStreamName, tuple, new Values(url, metadata,
                Status.FETCHED));
        _collector.ack(tuple);

    }

}
