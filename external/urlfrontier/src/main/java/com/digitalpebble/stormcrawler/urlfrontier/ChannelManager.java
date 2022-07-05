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

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manages the connections on a single machine. */
final class ChannelManager {

    private static final Logger LOG = LoggerFactory.getLogger(ChannelManager.class);

    private ChannelManager() {}

    private static final Object lock = new Object();

    private static final HashMap<String, ManagedChannelTuple> channels = new HashMap<>();

    @Contract("null -> false")
    private static boolean validChannelInTuple(@Nullable ManagedChannelTuple tuple) {
        return tuple != null && !tuple.channel.isShutdown() && !tuple.channel.isShutdown();
    }

    @NotNull
    private static ManagedChannel getOrCreateChannel(@NotNull String address) {
        ManagedChannelTuple result = channels.get(address);
        if (validChannelInTuple(result)) {
            LOG.debug("Found a channel with {} uses.", result.get());
            result.inc();
            return result.channel;
        }
        synchronized (lock) {
            result = channels.get(address);
            if (validChannelInTuple(result)) {
                LOG.debug("Found a channel with {} uses.", result.get());
                result.inc();
                return result.channel;
            }
            result =
                    new ManagedChannelTuple(
                            ManagedChannelBuilder.forTarget(address).usePlaintext().build());
            channels.put(address, result);
            LOG.debug("Created a new channel.");
            result.inc();
            return result.channel;
        }
    }

    /** Gets a channel for the given host and post. */
    @NotNull
    static ManagedChannel getChannel(@NotNull String host, @Range(from = 0, to = 65535) int port) {
        return getOrCreateChannel(host + ":" + port);
    }

    /** Gets a channel for the given address. */
    @NotNull
    static ManagedChannel getChannel(@NotNull String address) {
        return getOrCreateChannel(address);
    }

    /** Returns the channel and probably closes it. */
    static void returnChannel(@NotNull ManagedChannel channel) {
        for (var entry : channels.entrySet()) {
            var value = entry.getValue();
            if (channel == value.channel) {
                if (value.dec() == 0) {
                    synchronized (lock) {
                        if (value.get() == 0) {
                            if (!value.channel.isShutdown()) {
                                LOG.info("Shutting down channel {}", channel);
                                value.channel.shutdown();
                            } else {
                                LOG.warn(
                                        "The channel {} was already shut down but should not.",
                                        channel);
                            }
                            LOG.debug("Removing channel for address {}", entry.getKey());
                            channels.remove(entry.getKey());
                        }
                    }
                }
                return;
            }
        }
        LOG.warn(
                "The returned channel {} was not managed by the ChannelManager. Was it already closed?",
                channel);
    }

    /** Closes all managed channels */
    static void closeAll() {
        synchronized (lock) {
            for (var entry : channels.entrySet()) {
                var value = entry.getValue();
                var state = value.get();
                LOG.info(
                        "Shutting down and removing channel {} with address {}",
                        value,
                        entry.getKey());
                value.channel.shutdown();
                channels.remove(entry.getKey());
                if (state != 0) {
                    LOG.warn(
                            "The channel {} with address {} was shut down but is used by {} other objects.",
                            value,
                            entry.getKey(),
                            value.get());
                }
            }
        }
    }

    @SuppressWarnings("UnusedReturnValue")
    private static class ManagedChannelTuple {
        @NotNull ManagedChannel channel;
        @NotNull private final AtomicInteger usedBy = new AtomicInteger(0);

        public ManagedChannelTuple(@NotNull ManagedChannel channel) {
            this.channel = channel;
        }

        int get() {
            return usedBy.get();
        }

        int inc() {
            return usedBy.incrementAndGet();
        }

        int dec() {
            return usedBy.decrementAndGet();
        }
    }
}
