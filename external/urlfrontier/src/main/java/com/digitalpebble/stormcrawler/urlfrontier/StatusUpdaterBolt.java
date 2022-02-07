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

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.persistence.AbstractStatusUpdaterBolt;
import com.digitalpebble.stormcrawler.persistence.Status;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import com.digitalpebble.stormcrawler.util.URLPartitioner;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import crawlercommons.urlfrontier.URLFrontierGrpc;
import crawlercommons.urlfrontier.URLFrontierGrpc.URLFrontierStub;
import crawlercommons.urlfrontier.Urlfrontier.DiscoveredURLItem;
import crawlercommons.urlfrontier.Urlfrontier.KnownURLItem;
import crawlercommons.urlfrontier.Urlfrontier.StringList;
import crawlercommons.urlfrontier.Urlfrontier.StringList.Builder;
import crawlercommons.urlfrontier.Urlfrontier.URLInfo;
import crawlercommons.urlfrontier.Urlfrontier.URLItem;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatusUpdaterBolt extends AbstractStatusUpdaterBolt
        implements RemovalListener<String, List<Tuple>>,
                StreamObserver<crawlercommons.urlfrontier.Urlfrontier.String> {

    public static final Logger LOG = LoggerFactory.getLogger(StatusUpdaterBolt.class);
    private URLFrontierStub frontier;
    private ManagedChannel channel;
    private URLPartitioner partitioner;
    private StreamObserver<URLItem> requestObserver;
    private Cache<String, List<Tuple>> waitAck;

    private int maxMessagesinFlight = 100000;
    private AtomicInteger messagesinFlight = new AtomicInteger();

    public StatusUpdaterBolt() {
        waitAck =
                Caffeine.newBuilder()
                        .expireAfterWrite(60, TimeUnit.SECONDS)
                        .removalListener(this)
                        .build();
    }

    @Override
    public void prepare(
            Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        String host = ConfUtils.getString(stormConf, "urlfrontier.host", "localhost");
        int port = ConfUtils.getInt(stormConf, "urlfrontier.port", 7071);

        maxMessagesinFlight =
                ConfUtils.getInt(
                        stormConf, "urlfrontier.updater.max.messages", maxMessagesinFlight);

        LOG.info("Initialisation of connection to URLFrontier service on {}:{}", host, port);
        LOG.info("Allowing up to {} message in flight", maxMessagesinFlight);

        channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        frontier = URLFrontierGrpc.newStub(channel);

        partitioner = new URLPartitioner();
        partitioner.configure(stormConf);

        requestObserver = frontier.putURLs(this);
    }

    @Override
    public void onNext(final crawlercommons.urlfrontier.Urlfrontier.String value) {
        final String url = value.getValue();
        synchronized (waitAck) {
            List<Tuple> xx = waitAck.getIfPresent(url);
            if (xx != null) {
                waitAck.invalidate(url);
            } else {
                LOG.warn("Could not find unacked tuple for {}", url);
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
            String url, Status status, Metadata metadata, Optional<Date> nextFetch, Tuple t)
            throws Exception {

        while (messagesinFlight.get() >= this.maxMessagesinFlight) {
            LOG.debug("{} messages in flight - waiting a bit...", messagesinFlight.get());
            Utils.sleep(100);
        }

        // only 1 thread at a time will access the store method
        // but onNext() might try to access waitAck at the same time
        synchronized (waitAck) {

            // tuples received for the same URL
            // could be the same URL discovered from different pages
            // at the same time
            // or a page fetched linking to itself
            List<Tuple> tt = waitAck.get(url, k -> new LinkedList<Tuple>());

            // check that the same URL is not being sent to the frontier
            if (status.equals(Status.DISCOVERED) && !tt.isEmpty()) {
                // if this object is discovered - adding another version of it
                // won't make any difference
                LOG.debug("Already being sent to urlfrontier {} with status {}", url, status);
                // ack straight away!
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
                Builder builder = StringList.newBuilder();
                for (String v : vals) builder.addValues(v);
                mdCopy.put(k, builder.build());
            }

            URLInfo info =
                    URLInfo.newBuilder()
                            .setKey(partitionKey)
                            .setUrl(url)
                            .putAllMetadata(mdCopy)
                            .build();

            crawlercommons.urlfrontier.Urlfrontier.URLItem.Builder itemBuilder =
                    URLItem.newBuilder();
            if (status.equals(Status.DISCOVERED)) {
                itemBuilder.setDiscovered(DiscoveredURLItem.newBuilder().setInfo(info).build());
            } else {
                // next fetch date
                long date = 0;
                if (nextFetch.isPresent()) {
                    date = nextFetch.get().toInstant().getEpochSecond();
                }
                itemBuilder.setKnown(
                        KnownURLItem.newBuilder()
                                .setInfo(info)
                                .setRefetchableFromDate(date)
                                .build());
            }

            messagesinFlight.incrementAndGet();
            requestObserver.onNext(itemBuilder.build());

            tt.add(t);
            LOG.trace("Added to waitAck {} with ID {} total {}", url, url, tt.size());
        }
    }

    @Override
    public void onRemoval(
            @Nullable String key, @Nullable List<Tuple> values, @NotNull RemovalCause cause) {

        // explicit removal
        if (!cause.wasEvicted()) {
            LOG.debug("Acked {} tuple(s) for ID {}", values.size(), key);
            for (Tuple x : values) {
                messagesinFlight.decrementAndGet();
                super.ack(x, key);
            }
            return;
        }

        LOG.error("Evicted {} from waitAck with {} values", key, values.size());

        for (Tuple t : values) {
            messagesinFlight.decrementAndGet();
            _collector.fail(t);
        }
    }

    @Override
    public void cleanup() {
        requestObserver.onCompleted();
        channel.shutdownNow();
    }
}
