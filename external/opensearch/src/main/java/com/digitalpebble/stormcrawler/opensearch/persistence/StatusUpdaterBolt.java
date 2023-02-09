/**
 * Licensed to DigitalPebble Ltd under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership.
 * DigitalPebble licenses this file to You under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.digitalpebble.stormcrawler.opensearch.persistence;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.opensearch.BulkItemResponseToFailedFlag;
import com.digitalpebble.stormcrawler.opensearch.Constants;
import com.digitalpebble.stormcrawler.opensearch.IndexCreation;
import com.digitalpebble.stormcrawler.opensearch.OpensearchConnection;
import com.digitalpebble.stormcrawler.persistence.AbstractStatusUpdaterBolt;
import com.digitalpebble.stormcrawler.persistence.Status;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import com.digitalpebble.stormcrawler.util.URLPartitioner;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkProcessor;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.rest.RestStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple bolt which stores the status of URLs into ElasticSearch. Takes the tuples coming from the
 * 'status' stream. To be used in combination with a Spout to read from the index.
 */
public class StatusUpdaterBolt extends AbstractStatusUpdaterBolt
        implements RemovalListener<String, List<Tuple>>, BulkProcessor.Listener {

    private static final Logger LOG = LoggerFactory.getLogger(StatusUpdaterBolt.class);

    private String ESBoltType = "status";

    private static final String ESStatusIndexNameParamName =
            Constants.PARAMPREFIX + "%s.index.name";
    private static final String ESStatusRoutingParamName = Constants.PARAMPREFIX + "%s.routing";
    private static final String ESStatusRoutingFieldParamName =
            Constants.PARAMPREFIX + "%s.routing.fieldname";

    private boolean routingFieldNameInMetadata = false;

    private String indexName;

    private URLPartitioner partitioner;

    /** whether to apply the same partitioning logic used for politeness for routing, e.g byHost */
    private boolean doRouting;

    /** Store the key used for routing explicitly as a field in metadata * */
    private String fieldNameForRoutingKey = null;

    private OpensearchConnection connection;

    private Cache<String, List<Tuple>> waitAck;

    // Be fair due to cache timeout
    private final ReentrantLock waitAckLock = new ReentrantLock(true);

    private MultiCountMetric eventCounter;

    public StatusUpdaterBolt() {
        super();
    }

    /**
     * Loads the configuration using a substring different from the default value 'status' in order
     * to distinguish it from the spout configurations
     */
    public StatusUpdaterBolt(String boltType) {
        super();
        ESBoltType = boltType;
    }

    @Override
    public void prepare(
            Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {

        super.prepare(stormConf, context, collector);

        indexName =
                ConfUtils.getString(
                        stormConf,
                        String.format(StatusUpdaterBolt.ESStatusIndexNameParamName, ESBoltType),
                        "status");

        doRouting =
                ConfUtils.getBoolean(
                        stormConf,
                        String.format(StatusUpdaterBolt.ESStatusRoutingParamName, ESBoltType),
                        false);

        partitioner = new URLPartitioner();
        partitioner.configure(stormConf);

        fieldNameForRoutingKey =
                ConfUtils.getString(
                        stormConf,
                        String.format(StatusUpdaterBolt.ESStatusRoutingFieldParamName, ESBoltType));
        if (StringUtils.isNotBlank(fieldNameForRoutingKey)) {
            if (fieldNameForRoutingKey.startsWith("metadata.")) {
                routingFieldNameInMetadata = true;
                fieldNameForRoutingKey = fieldNameForRoutingKey.substring("metadata.".length());
            }
            // periods are not allowed in ES2 - replace with %2E
            fieldNameForRoutingKey = fieldNameForRoutingKey.replaceAll("\\.", "%2E");
        }

        waitAck =
                Caffeine.newBuilder()
                        .expireAfterWrite(60, TimeUnit.SECONDS)
                        .removalListener(this)
                        .build();

        // create gauge for waitAck
        context.registerMetric("waitAck", () -> waitAck.estimatedSize(), 10);

        try {
            connection = OpensearchConnection.getConnection(stormConf, ESBoltType, this);
        } catch (Exception e1) {
            LOG.error("Can't connect to ElasticSearch", e1);
            throw new RuntimeException(e1);
        }

        this.eventCounter = context.registerMetric("counters", new MultiCountMetric(), 30);

        // use the default status schema if none has been specified
        try {
            IndexCreation.checkOrCreateIndex(connection.getClient(), indexName, LOG);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void cleanup() {
        if (connection == null) {
            return;
        }
        connection.close();
        connection = null;
    }

    @Override
    public void store(
            String url, Status status, Metadata metadata, Optional<Date> nextFetch, Tuple tuple)
            throws Exception {

        String documentID = getDocumentID(metadata, url);

        boolean isAlreadySentAndDiscovered;
        // need to synchronize: otherwise it might get added to the cache
        // without having been sent to ES
        waitAckLock.lock();
        try {
            // check that the same URL is not being sent to ES
            final var alreadySent = waitAck.getIfPresent(documentID);
            isAlreadySentAndDiscovered = status.equals(Status.DISCOVERED) && alreadySent != null;
        } finally {
            waitAckLock.unlock();
        }

        if (isAlreadySentAndDiscovered) {
            // if this object is discovered - adding another version of it
            // won't make any difference
            LOG.debug(
                    "Already being sent to ES {} with status {} and ID {}",
                    url,
                    status,
                    documentID);
            // ack straight away!
            eventCounter.scope("acked").incrBy(1);
            super.ack(tuple, url);
            return;
        }

        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        builder.field("url", url);
        builder.field("status", status);

        builder.startObject("metadata");
        for (String mdKey : metadata.keySet()) {
            String[] values = metadata.getValues(mdKey);
            // periods are not allowed in ES2 - replace with %2E
            mdKey = mdKey.replaceAll("\\.", "%2E");
            builder.array(mdKey, values);
        }

        String partitionKey = partitioner.getPartition(url, metadata);
        if (partitionKey == null) {
            partitionKey = "_DEFAULT_";
        }

        // store routing key in metadata?
        if (StringUtils.isNotBlank(fieldNameForRoutingKey) && routingFieldNameInMetadata) {
            builder.field(fieldNameForRoutingKey, partitionKey);
        }

        builder.endObject();

        // store routing key outside metadata?
        if (StringUtils.isNotBlank(fieldNameForRoutingKey) && !routingFieldNameInMetadata) {
            builder.field(fieldNameForRoutingKey, partitionKey);
        }

        if (nextFetch.isPresent()) {
            builder.timeField("nextFetchDate", nextFetch.get());
        }

        builder.endObject();

        IndexRequest request = new IndexRequest(getIndexName(metadata));

        // check that we don't overwrite an existing entry
        // When create is used, the index operation will fail if a document
        // by that id already exists in the index.
        final boolean create = status.equals(Status.DISCOVERED);
        request.source(builder).id(documentID).create(create);

        if (doRouting) {
            request.routing(partitionKey);
        }

        waitAckLock.lock();
        try {
            final List<Tuple> tt = waitAck.get(documentID, k -> new LinkedList<>());
            tt.add(tuple);
            LOG.debug("Added to waitAck {} with ID {} total {}", url, documentID, tt.size());
        } finally {
            waitAckLock.unlock();
        }

        LOG.debug("Sending to ES buffer {} with ID {}", url, documentID);

        connection.addToProcessor(request);
    }

    @Override
    public void onRemoval(
            @Nullable String key, @Nullable List<Tuple> value, @NotNull RemovalCause cause) {
        if (!cause.wasEvicted()) return;
        LOG.error("Purged from waitAck {} with {} values", key, value.size());
        for (Tuple t : value) {
            eventCounter.scope("failed").incrBy(1);
            _collector.fail(t);
        }
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
        LOG.debug("afterBulk [{}] with {} responses", executionId, request.numberOfActions());
        eventCounter.scope("bulks_received").incrBy(1);
        eventCounter.scope("bulk_msec").incrBy(response.getTook().getMillis());

        var idsToBulkItemsWithFailedFlag =
                Arrays.stream(response.getItems())
                        .map(
                                bir -> {
                                    String id = bir.getId();
                                    BulkItemResponse.Failure f = bir.getFailure();
                                    boolean failed = false;
                                    if (f != null) {
                                        // already discovered
                                        if (f.getStatus().equals(RestStatus.CONFLICT)) {
                                            eventCounter.scope("doc_conflicts").incrBy(1);
                                            LOG.debug("Doc conflict ID {}", id);
                                        } else {
                                            LOG.error("Update ID {}, failure: {}", id, f);
                                            failed = true;
                                        }
                                    }
                                    return new BulkItemResponseToFailedFlag(bir, failed);
                                })
                        .collect(
                                // https://github.com/DigitalPebble/storm-crawler/issues/832
                                Collectors.groupingBy(
                                        idWithFailedFlagTuple -> idWithFailedFlagTuple.id,
                                        Collectors.toUnmodifiableList()));

        Map<String, List<Tuple>> presentTuples;
        long estimatedSize;
        Set<String> debugInfo = null;
        waitAckLock.lock();
        try {
            presentTuples = waitAck.getAllPresent(idsToBulkItemsWithFailedFlag.keySet());
            if (!presentTuples.isEmpty()) {
                waitAck.invalidateAll(presentTuples.keySet());
            }
            estimatedSize = waitAck.estimatedSize();
            // Only if we have to.
            if (LOG.isDebugEnabled() && estimatedSize > 0L) {
                debugInfo = new HashSet<>(waitAck.asMap().keySet());
            }
        } finally {
            waitAckLock.unlock();
        }

        int ackCount = 0;
        int failureCount = 0;

        for (var entry : presentTuples.entrySet()) {
            final var id = entry.getKey();
            final var associatedTuple = entry.getValue();
            final var bulkItemsWithFailedFlag = idsToBulkItemsWithFailedFlag.get(id);

            BulkItemResponseToFailedFlag selected;
            if (bulkItemsWithFailedFlag.size() == 1) {
                selected = bulkItemsWithFailedFlag.get(0);
            } else {
                // Fallback if there are multiple responses for the same id
                BulkItemResponseToFailedFlag tmp = null;
                var ctFailed = 0;
                for (var buwff : bulkItemsWithFailedFlag) {
                    if (tmp == null) {
                        tmp = buwff;
                    }
                    if (buwff.failed) ctFailed++;
                    else tmp = buwff;
                }
                if (ctFailed != bulkItemsWithFailedFlag.size()) {
                    LOG.warn(
                            "The id {} would result in an ack and a failure. Using only the ack for processing.",
                            id);
                }
                selected = Objects.requireNonNull(tmp);
            }

            if (associatedTuple != null) {
                LOG.debug("Acked {} tuple(s) for ID {}", associatedTuple.size(), id);
                for (Tuple tuple : associatedTuple) {
                    if (!selected.failed) {
                        String url = tuple.getStringByField("url");
                        ackCount++;
                        // ack and put in cache
                        LOG.debug("Acked {} with ID {}", url, id);
                        eventCounter.scope("acked").incrBy(1);
                        super.ack(tuple, url);
                    } else {
                        failureCount++;
                        eventCounter.scope("failed").incrBy(1);
                        _collector.fail(tuple);
                    }
                }
            } else {
                LOG.warn("Could not find unacked tuple for {}", id);
            }
        }

        LOG.info(
                "Bulk response [{}] : items {}, waitAck {}, acked {}, failed {}",
                executionId,
                idsToBulkItemsWithFailedFlag.size(),
                estimatedSize,
                ackCount,
                failureCount);
        if (debugInfo != null) {
            for (String kinaw : debugInfo) {
                LOG.debug("Still in wait ack after bulk response [{}] => {}", executionId, kinaw);
            }
        }
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request, Throwable throwable) {
        eventCounter.scope("bulks_received").incrBy(1);
        LOG.error("Exception with bulk {} - failing the whole lot ", executionId, throwable);

        final var failedIds =
                request.requests().stream()
                        .map(DocWriteRequest::id)
                        .collect(Collectors.toUnmodifiableSet());
        Map<String, List<Tuple>> failedTupleLists;
        waitAckLock.lock();
        try {
            failedTupleLists = waitAck.getAllPresent(failedIds);
            if (!failedTupleLists.isEmpty()) {
                waitAck.invalidateAll(failedTupleLists.keySet());
            }
        } finally {
            waitAckLock.unlock();
        }

        for (var id : failedIds) {
            var failedTuples = failedTupleLists.get(id);
            if (failedTuples != null) {
                LOG.debug("Failed {} tuple(s) for ID {}", failedTuples.size(), id);
                for (Tuple x : failedTuples) {
                    // fail it
                    eventCounter.scope("failed").incrBy(1);
                    _collector.fail(x);
                }
            } else {
                LOG.warn("Could not find unacked tuple for {}", id);
            }
        }
    }

    @Override
    public void beforeBulk(long executionId, BulkRequest request) {
        LOG.debug("beforeBulk {} with {} actions", executionId, request.numberOfActions());
        eventCounter.scope("bulks_received").incrBy(1);
    }

    /**
     * Must be overridden for implementing custom index names based on some metadata information By
     * Default, indexName coming from config is used
     */
    protected String getIndexName(Metadata m) {
        return indexName;
    }
}
