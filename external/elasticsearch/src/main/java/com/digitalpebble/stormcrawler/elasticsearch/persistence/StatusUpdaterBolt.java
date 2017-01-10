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

package com.digitalpebble.stormcrawler.elasticsearch.persistence;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.elasticsearch.ElasticSearchConnection;
import com.digitalpebble.stormcrawler.persistence.AbstractStatusUpdaterBolt;
import com.digitalpebble.stormcrawler.persistence.Status;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import com.digitalpebble.stormcrawler.util.URLPartitioner;

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

    private boolean routingFieldNameInMetadata = false;

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

    private ConcurrentHashMap<String, Tuple[]> waitAck = new ConcurrentHashMap<>();

    private MultiCountMetric eventCounter;

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
            if (StringUtils.isNotBlank(fieldNameForRoutingKey)) {
                if (fieldNameForRoutingKey.startsWith("metadata.")) {
                    routingFieldNameInMetadata = true;
                    fieldNameForRoutingKey = fieldNameForRoutingKey
                            .substring("metadata.".length());
                }
                // periods are not allowed in ES2 - replace with %2E
                fieldNameForRoutingKey = fieldNameForRoutingKey.replaceAll(
                        "\\.", "%2E");
            }
        }

        // create gauge for waitAck
        context.registerMetric("waitAck", new IMetric() {
            @Override
            public Object getValueAndReset() {
                return waitAck.size();
            }
        }, 30);

        /** Custom listener so that we can control the bulk responses **/
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void afterBulk(long executionId, BulkRequest request,
                    BulkResponse response) {
                long msec = response.getTookInMillis();
                eventCounter.scope("bulks_received").incrBy(1);
                eventCounter.scope("bulk_msec").incrBy(msec);
                Iterator<BulkItemResponse> bulkitemiterator = response
                        .iterator();
                int itemcount = 0;
                int acked = 0;
                while (bulkitemiterator.hasNext()) {
                    BulkItemResponse bir = bulkitemiterator.next();
                    itemcount++;
                    String id = bir.getId();
                    Tuple[] xx = waitAck.remove(id);
                    if (xx != null) {
                        for (Tuple x : xx) {
                            LOG.debug("Removed from unacked {}", id);
                            acked++;
                            // ack and put in cache
                            StatusUpdaterBolt.super.ack(x, id);
                        }
                    } else {
                        LOG.warn("Could not find unacked tuple for {}", id);
                    }
                }

                LOG.info("Bulk response {}, waitAck {}, acked {}", itemcount,
                        waitAck.size(), acked);
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request,
                    Throwable throwable) {
                eventCounter.scope("bulks_received").incrBy(1);
                LOG.error("Exception with bulk {} - failing the whole lot ",
                        executionId, throwable);
                // WHOLE BULK FAILED
                // mark all the docs as fail
                Iterator<ActionRequest> itreq = request.requests().iterator();
                while (itreq.hasNext()) {
                    IndexRequest bir = (IndexRequest) itreq.next();
                    String id = bir.id();
                    Tuple[] xx = waitAck.remove(id);
                    if (xx != null) {
                        for (Tuple x : xx) {
                            LOG.debug("Removed from unacked {}", id);
                            // fail it
                            StatusUpdaterBolt.super._collector.fail(x);
                        }
                    } else {
                        LOG.warn("Could not find unacked tuple for {}", id);
                    }
                }
            }

            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                LOG.debug("beforeBulk {} with {} actions", executionId,
                        request.numberOfActions());
                eventCounter.scope("bulks_received").incrBy(1);
            }
        };

        try {
            connection = ElasticSearchConnection.getConnection(stormConf,
                    ESBoltType, listener);
        } catch (Exception e1) {
            LOG.error("Can't connect to ElasticSearch", e1);
            throw new RuntimeException(e1);
        }

        this.eventCounter = context.registerMetric("counters",
                new MultiCountMetric(), 30);
    }

    @Override
    public void cleanup() {
        if (connection != null)
            connection.close();
    }

    @Override
    public void store(String url, Status status, Metadata metadata,
            Date nextFetch) throws Exception {

        // check that the same URL is not being sent to ES
        if (waitAck.get(url) != null) {
            // if this object is discovered - adding another version of it won't
            // make any difference
            LOG.trace("Already being sent to ES {} with status {} ", url,
                    status);
            if (status.equals(Status.DISCOVERED)) {
                return;
            }
        }

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
            // periods are not allowed in ES2 - replace with %2E
            mdKey = mdKey.replaceAll("\\.", "%2E");
            builder.array(mdKey, values);
        }

        // store routing key in metadata?
        if (StringUtils.isNotBlank(partitionKey)
                && StringUtils.isNotBlank(fieldNameForRoutingKey)
                && routingFieldNameInMetadata) {
            builder.field(fieldNameForRoutingKey, partitionKey);
        }

        builder.endObject();

        // store routing key outside metadata?
        if (StringUtils.isNotBlank(partitionKey)
                && StringUtils.isNotBlank(fieldNameForRoutingKey)
                && !routingFieldNameInMetadata) {
            builder.field(fieldNameForRoutingKey, partitionKey);
        }

        builder.field("nextFetchDate", nextFetch);

        builder.endObject();

        String sha256hex = org.apache.commons.codec.digest.DigestUtils
                .sha256Hex(url);

        IndexRequestBuilder request = connection.getClient()
                .prepareIndex(indexName, docType).setSource(builder)
                .setCreate(create).setId(sha256hex);

        if (StringUtils.isNotBlank(partitionKey)) {
            request.setRouting(partitionKey);
        }

        connection.getProcessor().add(request.request());

        LOG.debug("Sent to ES buffer {}", url);
    }

    /**
     * Do not ack the tuple straight away! wait to get the confirmation that it
     * worked
     **/
    public void ack(Tuple t, String url) {
        synchronized (waitAck) {
            LOG.debug("in waitAck {}", url);
            String sha256hex = org.apache.commons.codec.digest.DigestUtils
                    .sha256Hex(url);
            Tuple[] tt = waitAck.get(sha256hex);
            if (tt == null) {
                tt = new Tuple[] { t };
            } else {
                Tuple[] tt2 = new Tuple[tt.length + 1];
                System.arraycopy(tt, 0, tt2, 0, tt.length);
                tt2[tt.length] = t;
                tt = tt2;
            }
            waitAck.put(sha256hex, tt);
        }
    }

}