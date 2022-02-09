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

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public enum PartitionMode {
    QUEUE_MODE_HOST("byHost"),
    QUEUE_MODE_DOMAIN("byDomain"),
    QUEUE_MODE_IP("byIP");

    public final String label;

    PartitionMode(@NotNull String label) {
        this.label = label;
    }

    @Nullable
    @Contract("null -> null")
    public static PartitionMode parseQueueModeLabel(@Nullable String label) {
        if (label == null) return null;
        for (PartitionMode tmp : PartitionMode.values()) {
            if (tmp.label.equalsIgnoreCase(label) || tmp.toString().equalsIgnoreCase(label)) {
                return tmp;
            }
        }
        return null;
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
}
