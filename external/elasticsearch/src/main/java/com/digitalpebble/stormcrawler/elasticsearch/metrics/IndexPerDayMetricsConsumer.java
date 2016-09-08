package com.digitalpebble.stormcrawler.elasticsearch.metrics;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Elasticsearch metrics consumer that writes an index per day.
 * <p/>
 * This addresses the deprecation of the TTL functionality. The per-day indices can
 * be managed using ES Curator.
 */
public class IndexPerDayMetricsConsumer extends MetricsConsumer {

    private static final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    @Override
    protected String getIndexName() {
        return new StringBuilder()
                .append(super.getIndexName())
                .append("-")
                .append(dateFormat.format(new Date()))
                .toString();
    }
}
