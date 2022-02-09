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
package com.digitalpebble.stormcrawler.util;

import crawlercommons.domains.PaidLevelDomain;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Locale;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.slf4j.LoggerFactory;

public final class PartitionUtil {

    // TODO: Cache for IP resolver? What about changing IP over time?

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(PartitionUtil.class);

    private PartitionUtil() {}

    /** Default config key for the queue mode of partitioner */
    public static final String PARTITION_MODE_PARAM_NAME = "partition.url.mode";
    /** Default config key for the queue mode of fetcher */
    public static final String DEFAULT_PARTITION_MODE_FOR_FETCHER_KEY = "fetcher.queue.mode";
    /** Default fallback value for the queue mode */
    public static final PartitionMode DEFAULT_FALLBACK_PARTITION_MODE =
            PartitionMode.QUEUE_MODE_HOST;

    /**
     * Reads the {@link PartitionMode} from the given {@code stormConf} with the provided {@code
     * partitionModeKey}. If the value at {@code partitionModeKey} is null or invalid it returns the
     * {@code fallback}.
     *
     * @param stormConf reference to the config.
     * @param partitionModeKey config key to the queue mode.
     * @param fallback fallback value if the value at {@code partitionModeKey} is not valid.
     * @return the extracted {@link PartitionMode}
     */
    @NotNull
    public static PartitionMode readPartitionMode(
            @NotNull Map<String, Object> stormConf,
            @NotNull String partitionModeKey,
            @NotNull PartitionMode fallback) {
        Object partitionModeValue = stormConf.get(partitionModeKey);

        PartitionMode partitionMode;

        // In some cases the partitionmode is set as enum value
        if (partitionModeValue instanceof PartitionMode) {
            partitionMode = (PartitionMode) partitionModeValue;
        } else {
            partitionMode = PartitionMode.parseQueueModeLabel((String) partitionModeValue);
        }

        if (partitionMode == null) {
            LOG.error(
                    "Unknown partition mode : {} - forcing to {}",
                    partitionModeValue,
                    fallback.label);
            partitionMode = fallback;
        }

        return partitionMode;
    }

    /**
     * Reads the {@link PartitionMode} from the given {@code stormConf} with the default value
     * {@value DEFAULT_PARTITION_MODE_FOR_FETCHER_KEY} and {@code defaultFallbackQueueMode}.
     *
     * @param stormConf reference to the config
     * @return the extracted {@link PartitionMode}
     */
    @NotNull
    public static PartitionMode readPartitionModeForFetcher(
            @NotNull Map<String, Object> stormConf) {
        return readPartitionMode(
                stormConf, DEFAULT_PARTITION_MODE_FOR_FETCHER_KEY, DEFAULT_FALLBACK_PARTITION_MODE);
    }

    /**
     * Reads the {@link PartitionMode} from the given {@code stormConf} with the default value
     * {@value DEFAULT_PARTITION_MODE_FOR_FETCHER_KEY} and {@code defaultFallbackQueueMode}.
     *
     * @param stormConf reference to the config
     * @return the extracted {@link PartitionMode}
     */
    @NotNull
    public static PartitionMode readPartitionModeForPartitioner(
            @NotNull Map<String, Object> stormConf) {
        return readPartitionMode(
                stormConf, PARTITION_MODE_PARAM_NAME, DEFAULT_FALLBACK_PARTITION_MODE);
    }

    /**
     * Gets the key for the fetcher queue from the given {@code url} by the provided {@code
     * queueMode}.
     *
     * @param url used for the key extraction.
     * @param partitionMode describing the required key information
     * @return a string value from the {@code url} for a queue.
     */
    @NotNull
    public static String getPartitionKeyByMode(
            @NotNull URL url, @NotNull PartitionMode partitionMode) {
        // Origin: FetchItem.create

        String key = null;

        switch (partitionMode) {
            case QUEUE_MODE_IP:
                try {
                    final InetAddress addr = InetAddress.getByName(url.getHost());
                    key = addr.getHostAddress();
                } catch (final UnknownHostException e) {
                    LOG.warn("Unable to resolve IP for {}, using hostname as key.", url.getHost());
                    key = url.getHost();
                }
                break;
            case QUEUE_MODE_DOMAIN:
                key = PaidLevelDomain.getPLD(url.getHost());
                if (key == null) {
                    LOG.warn("Unknown domain for url: {}, using hostname as key.", url);
                    key = url.getHost();
                }
                break;
            case QUEUE_MODE_HOST:
                key = url.getHost();
                break;
        }

        if (key == null) {
            LOG.warn("Unknown host for url: {}, using URL string as key", url);
            key = url.toExternalForm();
        }

        return key.toLowerCase(Locale.ROOT);
    }
}
