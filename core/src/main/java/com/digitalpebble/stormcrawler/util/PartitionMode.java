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

import java.util.HashMap;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.LoggerFactory;

public enum PartitionMode {
    QUEUE_MODE_HOST("byHost"),
    QUEUE_MODE_DOMAIN("byDomain"),
    QUEUE_MODE_IP("byIP");

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(PartitionMode.class);

    /** The label of the mode */
    public final @NotNull String label;

    PartitionMode(@NotNull String label) {
        this.label = label;
    }

    @Nullable
    @Contract("null -> null")
    public static PartitionMode parseQueueModeLabel(@Nullable String label) {
        if (label == null) return null;
        return lookup.get(label.toLowerCase());
    }

    @NotNull
    public static PartitionMode parseQueueModeLabelOrDefault(
            @Nullable String label, @NotNull PartitionMode defaultValue) {
        PartitionMode parsed = parseQueueModeLabel(label);
        if (parsed == null) {
            return defaultValue;
        }
        return parsed;
    }

    /*
     * A size of 16 with a load-factor of 1.0 is enough for a HashMap to contain the hashes of the
     * three name() and label values without collision.
     * You can check by this:
     *
     * // Code from java.util.HashMap 10.02.2022
     * static int hash(Object key) {
     *     int h;
     *     return (key == null) ? 0 : (h = key.hashCode()) ^ (h >>> 16);
     * }
     * public static void main(String[] args){
     *     int internalArrayLength = 16;
     *     int[] hits = new int[internalArrayLength];
     *     for (PartitionMode value : PartitionMode.values()) {
     *         hits[(hits.length - 1) & hash(value.label.toLowerCase())]++;
     *         hits[(hits.length - 1) & hash(value.name().toLowerCase())]++;
     *     }
     *     for (int i = 0; i < hits.length; i++) {
     *         System.out.println(i+": "+hits[i]);
     *     }
     *     int expectedNumberOfHits = PartitionMode.values().length*2;
     *     int actualNumberOfUniqueHits = (int) java.util.Arrays.stream(hits).filter(value -> value == 1).count();
     *     int expectedNumberOfNoHits = hits.length - expectedNumberOfHits;
     *     int actualNumberOfNoHits = (int) java.util.Arrays.stream(hits).filter(value -> value == 0).count();
     *     System.out.println("Got "+actualNumberOfUniqueHits+" out of "+ expectedNumberOfHits + " expected hits.");
     *     System.out.println("Got "+actualNumberOfNoHits+" out of "+ expectedNumberOfNoHits + " expected non hits.");
     * }
     */
    private static final HashMap<String, PartitionMode> lookup = new HashMap<>(16, 1.0f);

    static {
        PartitionMode[] values = PartitionMode.values();
        PartitionMode tmp;
        for (PartitionMode mode : values) {
            if ((tmp = lookup.put(mode.name().toLowerCase(), mode)) != null) {
                LOG.warn(
                        "Replaces {} with {} due to same key value of the name {}",
                        tmp,
                        mode,
                        mode.name().toLowerCase());
            }
            if ((tmp = lookup.put(mode.label.toLowerCase(), mode)) != null) {
                LOG.warn(
                        "Replaces {} with {} due to same key value of the label {}",
                        tmp,
                        mode,
                        mode.label.toLowerCase());
            }
        }
    }
}
