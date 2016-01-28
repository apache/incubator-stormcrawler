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

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.storm.crawler.Metadata;
import com.digitalpebble.storm.crawler.elasticsearch.ElasticSearchConnection;
import com.digitalpebble.storm.crawler.persistence.AbstractStatusUpdaterBolt;
import com.digitalpebble.storm.crawler.persistence.Status;
import com.digitalpebble.storm.crawler.util.ConfUtils;
import com.digitalpebble.storm.crawler.util.URLPartitioner;

import backtype.storm.Config;
import backtype.storm.metric.api.IMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.TupleUtils;

/**
 * Simple bolt which stores the status of URLs into ElasticSearch. Takes the
 * tuples coming from the 'status' stream. To be used in combination with a
 * Spout to read from the index.
 **/
@SuppressWarnings("serial")
public class StatusUpdaterBolt extends AbstractStatusUpdaterBolt {

    private static final Logger LOG = LoggerFactory
            .getLogger(StatusUpdaterBolt.class);

    private static final String ESBoltType = "status";

    private static final String ESStatusIndexNameParamName = "es.status.index.name";
    private static final String ESStatusDocTypeParamName = "es.status.doc.type";
    private static final String ESStatusRoutingParamName = "es.status.routing";
    private static final String ESStatusRoutingFieldParamName = "es.status.routing.fieldname";

    private String indexName;
    private String docType;

    private URLPartitioner partitioner;

    /**
     * whether to apply the same partitioning logic used for politeness for
     * routing, e.g byHost
     **/
    private boolean doRouting;

    /** Store the key used for routing explicitly as a field in metadata **/
    private String fieldNameForRoutingKey = null;

    private ElasticSearchConnection connection;

    private ConcurrentHashMap<String, Tuple> unacked = new ConcurrentHashMap<String, Tuple>();
    private ConcurrentHashMap<String, Tuple> readytoack = new ConcurrentHashMap<String, Tuple>();

    @Override
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {

        super.prepare(stormConf, context, collector);

        indexName = ConfUtils.getString(stormConf,
                StatusUpdaterBolt.ESStatusIndexNameParamName, "status");
        docType = ConfUtils.getString(stormConf,
                StatusUpdaterBolt.ESStatusDocTypeParamName, "status");

        doRouting = ConfUtils.getBoolean(stormConf,
                StatusUpdaterBolt.ESStatusRoutingParamName, false);

        if (doRouting) {
            partitioner = new URLPartitioner();
            partitioner.configure(stormConf);
            fieldNameForRoutingKey = ConfUtils.getString(stormConf,
                    StatusUpdaterBolt.ESStatusRoutingFieldParamName);
        }

        /** Custom listener so that we can control the bulk responses **/
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void afterBulk(long executionId, BulkRequest request,
                    BulkResponse response) {
                // a failure is not necessarily anything sinister
                // could be for instance about DocumentAlreadyExistsException
                if (response.hasFailures()) {
                    LOG.debug("Failure with bulk {} : {}", executionId,
                            response.buildFailureMessage());
                }
                Iterator<BulkItemResponse> bulkitemiterator = response
                        .iterator();
                while (bulkitemiterator.hasNext()) {
                    BulkItemResponse bir = bulkitemiterator.next();
                    // TODO determine whether the failure is significant or not
                    String id = bir.getId();
                    Tuple x = unacked.remove(id);
                    // x should not be null;
                    if (x != null) {
                        readytoack.put(id, x);
                    } else {
                        LOG.error("Could not find unacked tuple for {}", id);
                    }
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request,
                    Throwable throwable) {
                LOG.error("Exception with bulk {} : ", executionId, throwable);
            }

            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                LOG.debug("beforeBulk {} with {} actions", executionId,
                        request.numberOfActions());
            }
        };

        try {
            connection = ElasticSearchConnection.getConnection(stormConf,
                    ESBoltType, listener);
        } catch (Exception e1) {
            LOG.error("Can't connect to ElasticSearch", e1);
            throw new RuntimeException(e1);
        }

        // create gauges
        context.registerMetric("unacked", new IMetric() {
            @Override
            public Object getValueAndReset() {
                return unacked.size();
            }
        }, 30);

        context.registerMetric("readytoack", new IMetric() {
            @Override
            public Object getValueAndReset() {
                return readytoack.size();
            }
        }, 30);
    }

    @Override
    public void cleanup() {
        if (connection != null)
            connection.close();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 1);
        return conf;
    }

    @Override
    public void execute(Tuple tuple) {
        ackbuffer();

        if (TupleUtils.isTick(tuple)) {
            _collector.ack(tuple);
            return;
        }

        super.execute(tuple);
    }

    /** Ack tuples **/
    private void ackbuffer() {
        // any tuples ready to ack ?
        while (readytoack.size() > 0) {
            Iterator<Entry<String, Tuple>> iter = readytoack.entrySet()
                    .iterator();
            Entry<String, Tuple> entry = iter.next();
            String url = entry.getKey();
            readytoack.remove(url);
            super.ack(entry.getValue(), url);
        }
    }

    @Override
    public void store(String url, Status status, Metadata metadata,
            Date nextFetch) throws Exception {

        String partitionKey = null;

        if (doRouting) {
            partitionKey = partitioner.getPartition(url, metadata);
        }

        XContentBuilder builder = jsonBuilder().startObject();
        builder.field("url", url);
        builder.field("status", status);

        // check that we don't overwrite an existing entry
        // When create is used, the index operation will fail if a document
        // by that id already exists in the index.
        boolean create = status.equals(Status.DISCOVERED);

        builder.startObject("metadata");
        Iterator<String> mdKeys = metadata.keySet().iterator();
        while (mdKeys.hasNext()) {
            String mdKey = mdKeys.next();
            String[] values = metadata.getValues(mdKey);
            builder.array(mdKey, values);
        }

        // store routing key in metadata?
        if (StringUtils.isNotBlank(partitionKey)
                && StringUtils.isNotBlank(fieldNameForRoutingKey)) {
            builder.field(fieldNameForRoutingKey, partitionKey);
        }

        builder.endObject();

        builder.field("nextFetchDate", nextFetch);

        builder.endObject();

        IndexRequestBuilder request = connection.getClient()
                .prepareIndex(indexName, docType).setSource(builder)
                .setCreate(create).setId(url);

        if (StringUtils.isNotBlank(partitionKey)) {
            request.setRouting(partitionKey);
        }

        connection.getProcessor().add(request.request());
    }

    /**
     * Do not ack the tuple straight away! wait to get the confirmation that it
     * worked
     **/
    public void ack(Tuple t, String url) {
        unacked.put(url, t);
    }

}
