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
package com.digitalpebble.stormcrawler.urlfrontier;

import static com.digitalpebble.stormcrawler.urlfrontier.Constants.*;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.persistence.AbstractStatusUpdaterBolt;
import com.digitalpebble.stormcrawler.persistence.Status;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import com.digitalpebble.stormcrawler.util.URLPartitioner;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import crawlercommons.urlfrontier.CrawlID;
import crawlercommons.urlfrontier.URLFrontierGrpc;
import crawlercommons.urlfrontier.URLFrontierGrpc.URLFrontierStub;
import crawlercommons.urlfrontier.Urlfrontier.AckMessage;
import crawlercommons.urlfrontier.Urlfrontier.DiscoveredURLItem;
import crawlercommons.urlfrontier.Urlfrontier.KnownURLItem;
import crawlercommons.urlfrontier.Urlfrontier.StringList;
import crawlercommons.urlfrontier.Urlfrontier.StringList.Builder;
import crawlercommons.urlfrontier.Urlfrontier.URLInfo;
import crawlercommons.urlfrontier.Urlfrontier.URLItem;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.tika.utils.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatusUpdaterBolt extends AbstractStatusUpdaterBolt
        implements RemovalListener<String, List<Tuple>>,
                StreamObserver<crawlercommons.urlfrontier.Urlfrontier.AckMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(StatusUpdaterBolt.class);

    private ManagedChannel channel;
    private URLPartitioner partitioner;
    private StreamObserver<URLItem> requestObserver;

    private Cache<String, List<Tuple>> waitAck;

    // We have to prevent starving caused by the cache. Therefore, sophisticated lock with fairness.
    private final ReentrantReadWriteLock waitAckLock = new ReentrantReadWriteLock(true);

    private int maxMessagesInFlight = 100000;
    private long throttleTime = 10;

    // Faster ways of locking until n messages are processed
    private Semaphore inFlightSemaphore;

    private MultiCountMetric eventCounter;

    /** Globally set crawlID * */
    private String globalCrawlID;

    @Override
    public void prepare(
            Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);

        long expireAfterNMillisec =
                ConfUtils.getLong(stormConf, URLFRONTIER_CACHE_EXPIREAFTER_MS_KEY, 60_000L);

        waitAck =
                Caffeine.newBuilder()
                        .expireAfterWrite(expireAfterNMillisec, TimeUnit.MILLISECONDS)
                        .removalListener(this)
                        .build();

        // host and port of URL Frontier(s)
        List<String> addresses = ConfUtils.loadListFromConf(URLFRONTIER_ADDRESS_KEY, stormConf);

        String address = null;

        if (addresses.size() > 1) {
            int totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
            // check that the number of tasks is a multiple of the frontier nodes
            if (totalTasks < addresses.size()) {
                String message =
                        "Needs at least one task per frontier node. "
                                + totalTasks
                                + " vs "
                                + addresses.size();
                LOG.error(message);
                throw new RuntimeException();
            }
            if (totalTasks % addresses.size() != 0) {
                String message =
                        "Number of tasks not a multiple of the number of frontier nodes. "
                                + totalTasks
                                + " vs "
                                + addresses.size();
                LOG.error(message);
                throw new RuntimeException();
            }

            int nodeIndex = context.getThisTaskIndex();
            int assignment = nodeIndex % addresses.size();
            Collections.sort(addresses);
            address = addresses.get(assignment);
        }

        if (address == null) {
            String host =
                    ConfUtils.getString(stormConf, URLFRONTIER_HOST_KEY, URLFRONTIER_DEFAULT_HOST);
            int port = ConfUtils.getInt(stormConf, URLFRONTIER_PORT_KEY, URLFRONTIER_DEFAULT_PORT);
            address = host + ":" + port;
        }

        maxMessagesInFlight =
                ConfUtils.getInt(
                        stormConf, URLFRONTIER_MAX_MESSAGES_IN_FLIGHT_KEY, maxMessagesInFlight);

        throttleTime =
                ConfUtils.getLong(stormConf, URLFRONTIER_THROTTLING_TIME_MS_KEY, throttleTime);

        this.eventCounter =
                context.registerMetric(this.getClass().getSimpleName(), new MultiCountMetric(), 30);

        maxMessagesInFlight =
                ConfUtils.getInt(
                        stormConf, URLFRONTIER_UPDATER_MAX_MESSAGES_KEY, maxMessagesInFlight);

        // Fairness not necessary, we are not in a hurry, as long as we may be processed at some
        // point.
        inFlightSemaphore = new Semaphore(maxMessagesInFlight, false);

        LOG.info("Initialisation of connection to URLFrontier service on {}", address);
        LOG.info("Allowing up to {} message in flight", maxMessagesInFlight);

        // add the default port if missing
        if (!address.contains(":")) {
            address += ":7071";
        }

        channel = ChannelManager.getChannel(address);
        URLFrontierStub frontier = URLFrontierGrpc.newStub(channel).withWaitForReady();

        partitioner = new URLPartitioner();
        partitioner.configure(stormConf);

        globalCrawlID = ConfUtils.getString(stormConf, URLFRONTIER_CRAWL_ID_KEY, CrawlID.DEFAULT);

        requestObserver = frontier.putURLs(this);
    }

    @Override
    public void onNext(final crawlercommons.urlfrontier.Urlfrontier.AckMessage confirmation) {
        // use the URL as ID
        final String url = confirmation.getID();

        List<Tuple> values;
        try {
            waitAckLock.readLock().lock();
            values = waitAck.getIfPresent(url);
        } finally {
            waitAckLock.readLock().unlock();
        }

        if (values == null) {
            if (StringUtils.isBlank(url)) {
                LOG.warn(
                        "Could not find unacked tuple for blank id `{}`. (Ack: {})",
                        url,
                        confirmation);
                if (LOG.isTraceEnabled()) {
                    final StringBuilder sb = new StringBuilder();
                    sb.append("Trace for unpacked tuple for blank id: ");
                    for (var entry : confirmation.getAllFields().entrySet()) {
                        sb.append("\n")
                                .append("ENTRY: ")
                                .append(entry.getKey().toString())
                                .append(" -> ")
                                .append(entry.getValue().toString());
                    }
                    LOG.trace(sb.toString());
                }
            } else {
                LOG.debug("Could not find unacked tuple for id `{}`.", url);
            }
            return;
        }

        try {
            waitAckLock.writeLock().lock();
            waitAck.invalidate(url);
        } finally {
            waitAckLock.writeLock().unlock();
        }

        final boolean hasFailed = confirmation.getStatus().equals(AckMessage.Status.FAIL);
        inFlightSemaphore.release(values.size());

        if (!hasFailed) {
            LOG.debug("Acked {} tuple(s) for ID {}", values.size(), url);
            for (Tuple t : values) {
                eventCounter.scope("acked").incrBy(1);
                super.ack(t, url);
            }
        } else {
            LOG.info("Failed {} tuple(s) for ID {}", values.size(), url);
            for (Tuple t : values) {
                eventCounter.scope("failed").incrBy(1);
                _collector.fail(t);
            }
        }
    }

    @Override
    public void onError(Throwable t) {
        LOG.error("Error received", t);
    }

    @Override
    public void onCompleted() {
        // end of stream - nothing special to do?
    }

    @Override
    public void store(
            @NotNull String url,
            @NotNull Status status,
            @NotNull Metadata metadata,
            @NotNull Optional<Date> nextFetch,
            @NotNull Tuple t) {

        var hasPermit = false;
        var timeSpent = 0L;

        while (!hasPermit) {
            try {
                hasPermit = inFlightSemaphore.tryAcquire(throttleTime, TimeUnit.MILLISECONDS);
                if (!hasPermit) {
                    LOG.debug(
                            "{} messages in flight, time spent throttling {}",
                            inFlightSemaphore.getQueueLength(),
                            timeSpent);
                    eventCounter.scope("timeSpentThrottling").incrBy(throttleTime);
                    timeSpent += throttleTime;
                    if (timeSpent >= 30000L) {
                        LOG.warn(
                                "Waiting more than {} ms for processing. There are {} permits available for {} waiting threads.",
                                timeSpent,
                                inFlightSemaphore.availablePermits(),
                                inFlightSemaphore.getQueueLength());
                    }
                }
            } catch (InterruptedException e) {
                LOG.info(
                        "InterruptedException - {} messages in flight",
                        inFlightSemaphore.getQueueLength());
            }
        }

        var sameURLNotSentToFrontier = false;

        // only 1 thread at a time will access the store method
        // but onNext() might try to access waitAck at the same time
        try {
            waitAckLock.writeLock().lock();

            // tuples received for the same URL
            // could be the same URL discovered from different pages
            // at the same time
            // or a page fetched linking to itself
            List<Tuple> tt = waitAck.get(url, k -> new LinkedList<>());

            // check that the same URL is not being sent to the frontier
            sameURLNotSentToFrontier = status.equals(Status.DISCOVERED) && !tt.isEmpty();

            if (!sameURLNotSentToFrontier) {
                tt.add(t);
                LOG.trace(
                        "Added to waitAck {} with ID {} total {} - sent to {}",
                        url,
                        url,
                        tt.size(),
                        channel.authority());
            }

        } finally {
            waitAckLock.writeLock().unlock();
        }

        if (sameURLNotSentToFrontier) {
            inFlightSemaphore.release();
            // if this object is discovered - adding another version of it
            // won't make any difference
            LOG.debug("Already being sent to urlfrontier {} with status {}", url, status);
            // ack straight away!
            eventCounter.scope("acked").incrBy(1);
            super.ack(t, url);
            return;
        }

        String partitionKey = partitioner.getPartition(url, metadata);
        if (partitionKey == null) {
            partitionKey = "_DEFAULT_";
        }

        final Map<String, StringList> mdCopy = new HashMap<>(metadata.size());
        for (String k : metadata.keySet()) {
            String[] vals = metadata.getValues(k);
            if (vals != null) {
                Builder builder = StringList.newBuilder();
                for (String v : vals) builder.addValues(v);
                mdCopy.put(k, builder.build());
            }
        }

        URLInfo info =
                URLInfo.newBuilder()
                        .setKey(partitionKey)
                        .setUrl(url)
                        .setCrawlID(globalCrawlID)
                        .putAllMetadata(mdCopy)
                        .build();

        crawlercommons.urlfrontier.Urlfrontier.URLItem.Builder itemBuilder = URLItem.newBuilder();
        if (status.equals(Status.DISCOVERED)) {
            itemBuilder.setDiscovered(DiscoveredURLItem.newBuilder().setInfo(info).build());
        } else {
            // next fetch date
            long date = 0;
            if (nextFetch.isPresent()) {
                date = nextFetch.get().toInstant().getEpochSecond();
            }
            itemBuilder.setKnown(
                    KnownURLItem.newBuilder().setInfo(info).setRefetchableFromDate(date).build());
        }

        requestObserver.onNext(itemBuilder.setID(url).build());
    }

    @Override
    public void onRemoval(
            @Nullable String key, @Nullable List<Tuple> values, @NotNull RemovalCause cause) {

        // explicit removal
        if (!cause.wasEvicted()) {
            if (values != null) {
                LOG.trace(
                        "Evicted {} from waitAck with {} values. [{}]", key, values.size(), cause);
            } else {
                LOG.trace("Evicted {} from waitAck with no values. [{}]", key, cause);
            }
            return;
        }

        if (values != null) {
            inFlightSemaphore.release(values.size());
            var permits = inFlightSemaphore.availablePermits();
            LOG.warn("Evicted {} from waitAck with {} values. [{}]", key, values.size(), cause);

            if (permits < 0) {
                LOG.warn(
                        "Removing more elements than possible, the semaphore is negative {}.",
                        permits);
            }

            for (Tuple t : values) {
                eventCounter.scope("failed").incrBy(1);
                _collector.fail(t);
            }
        } else {
            LOG.error("Evicted {} from waitAck with no values. [{}]", key, cause);
        }
    }

    @Override
    public void cleanup() {
        requestObserver.onCompleted();
        ChannelManager.returnChannel(channel);
    }
}
