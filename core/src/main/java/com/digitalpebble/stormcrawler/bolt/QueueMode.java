package com.digitalpebble.stormcrawler.bolt;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public enum QueueMode {
    QUEUE_MODE_HOST("byHost"),
    QUEUE_MODE_DOMAIN("byDomain"),
    QUEUE_MODE_IP("byIP");

    public final String label;

    QueueMode(@NotNull String label){
        this.label = label;
    }

    @Nullable
    @Contract("null -> null")
    public static QueueMode parseQueueModeLabel(@Nullable String label){
        if(label == null) return null;
        for (QueueMode tmp : QueueMode.values()) {
            if (tmp.label.equalsIgnoreCase(label)) {
                return tmp;
            }
        }
        return null;
    }

    @NotNull
    public static QueueMode parseQueueModeLabelOrDefault(@Nullable String label, @NotNull QueueMode defaultValue){
        QueueMode parsed = parseQueueModeLabel(label);
        if(parsed == null){
            return defaultValue;
        }
        return parsed;
    }

}
