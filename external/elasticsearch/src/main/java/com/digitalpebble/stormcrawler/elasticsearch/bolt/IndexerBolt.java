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

package com.digitalpebble.stormcrawler.elasticsearch.bolt;

import static com.digitalpebble.stormcrawler.Constants.StatusStreamName;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.metric.api.MultiReducedMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.elasticsearch.ElasticSearchConnection;
import com.digitalpebble.stormcrawler.indexing.AbstractIndexerBolt;
import com.digitalpebble.stormcrawler.persistence.Status;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import com.digitalpebble.stormcrawler.util.PerSecondReducer;

/**
 * Sends documents to ElasticSearch. Indexes all the fields from the tuples or a
 * Map &lt;String,Object&gt; from a named field.
 */
@SuppressWarnings("serial")
public class IndexerBolt extends AbstractIndexerBolt implements
        BulkProcessor.Listener {

    private static final Logger LOG = LoggerFactory
            .getLogger(IndexerBolt.class);

    private static final String ESBoltType = "indexer";

    static final String ESIndexNameParamName = "es.indexer.index.name";
    static final String ESDocTypeParamName = "es.indexer.doc.type";
    private static final String ESCreateParamName = "es.indexer.create";
    private static final String ESIndexPipelineParamName = "es.indexer.pipeline";

    private OutputCollector _collector;

    private String indexName;
    private String docType;

    private String pipeline;

    // whether the document will be created only if it does not exist or
    // overwritten
    private boolean create = false;

    private MultiCountMetric eventCounter;

    private ElasticSearchConnection connection;

    private MultiReducedMetric perSecMetrics;

    public IndexerBolt() {
    }

    /** Sets the index name instead of taking it from the configuration. **/
    public IndexerBolt(String indexName) {
        this.indexName = indexName;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void prepare(Map conf, TopologyContext context,
            OutputCollector collector) {
        super.prepare(conf, context, collector);
        _collector = collector;
        if (indexName == null) {
            indexName = ConfUtils.getString(conf,
                    IndexerBolt.ESIndexNameParamName, "content");
        }
        docType = ConfUtils.getString(conf, IndexerBolt.ESDocTypeParamName,
                "doc");
        create = ConfUtils.getBoolean(conf, IndexerBolt.ESCreateParamName,
                false);

        pipeline = ConfUtils.getString(conf,
                IndexerBolt.ESIndexPipelineParamName);

        try {
            connection = ElasticSearchConnection.getConnection(conf,
                    ESBoltType, this);
        } catch (Exception e1) {
            LOG.error("Can't connect to ElasticSearch", e1);
            throw new RuntimeException(e1);
        }

        this.eventCounter = context.registerMetric("ElasticSearchIndexer",
                new MultiCountMetric(), 10);

        this.perSecMetrics = context.registerMetric("Indexer_average_persec",
                new MultiReducedMetric(new PerSecondReducer()), 10);
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

        try {
            XContentBuilder builder = jsonBuilder().startObject();

            // display text of the document?
            if (fieldNameForText() != null) {
                builder.field(fieldNameForText(), trimText(text));
            }

            // send URL as field?
            if (fieldNameForURL() != null) {
                builder.field(fieldNameForURL(), normalisedurl);
            }

            // which metadata to display?
            Map<String, String[]> keyVals = filterMetadata(metadata);

            Iterator<String> iterator = keyVals.keySet().iterator();
            while (iterator.hasNext()) {
                String fieldName = iterator.next();
                String[] values = keyVals.get(fieldName);
                if (values.length == 1) {
                    builder.field(fieldName, values[0]);
                } else if (values.length > 1) {
                    builder.array(fieldName, values);
                }
            }

            builder.endObject();

            String sha256hex = org.apache.commons.codec.digest.DigestUtils
                    .sha256Hex(normalisedurl);

            IndexRequest indexRequest = new IndexRequest(
                    getIndexName(metadata), docType, sha256hex).source(builder);

            DocWriteRequest.OpType optype = DocWriteRequest.OpType.INDEX;

            if (create) {
                optype = DocWriteRequest.OpType.CREATE;
            }

            indexRequest.opType(optype);

            if (pipeline != null) {
                indexRequest.setPipeline(pipeline);
            }

            connection.getProcessor().add(indexRequest);

            eventCounter.scope("Indexed").incrBy(1);

            perSecMetrics.scope("Indexed").update(1);

            _collector.emit(StatusStreamName, tuple, new Values(url, metadata,
                    Status.FETCHED));
            _collector.ack(tuple);

        } catch (IOException e) {
            LOG.error("Error sending log tuple to ES", e);
            // do not send to status stream so that it gets replayed
            _collector.fail(tuple);
        }
    }

    /**
     * Must be overridden for implementing custom index names based on some
     * metadata information By Default, indexName coming from config is used
     */
    protected String getIndexName(Metadata m) {
        return indexName;
    }

    @Override
    public void beforeBulk(long executionId, BulkRequest request) {
        eventCounter.scope("BulkRequest").incrBy(1);
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request,
            BulkResponse response) {
        // TODO Auto-generated method stub
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request,
            Throwable failure) {
        // TODO Auto-generated method stub
    }

}
