package com.digitalpebble.stormcrawler.elasticsearch.metrics;

import java.text.DateFormat;
import java.util.Date;

/**
 * Elasticsearch metrics consumer that writes an index per time period. The time period is
 * specified by a date formatter supplied by concrete subclasses.
 * <p/>
 * This addresses the deprecation of the TTL functionality. The per-day indices can
 * be managed using ES Curator.
 */
abstract class IndexPerPeriodMetricsConsumer extends MetricsConsumer {

    /**
     * Date format used to append current time 'period' to the base metrics index for per-time period indexing.
     *
     * @return time period date formatter, e.g. "yyyy-MM-dd" for index per day
     */
    protected abstract DateFormat getDateFormat();

    @Override
    protected final String getIndexName() {
        return new StringBuilder()
                .append(super.getIndexName())
                .append("-")
                .append(getDateFormat().format(new Date()))
                .toString();
    }
}
