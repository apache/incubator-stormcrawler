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
