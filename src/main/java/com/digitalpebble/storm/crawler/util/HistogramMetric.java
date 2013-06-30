package com.digitalpebble.storm.crawler.util;


import backtype.storm.metric.api.IMetric;
import backtype.storm.metric.api.IReducer;
import backtype.storm.metric.api.ReducedMetric;
import com.codahale.metrics.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;


/**
 * @author Enno Shioji (enno.shioji@peerindex.com)
 */
public class HistogramMetric implements IMetric {

    private final MetricRegistry registry = new MetricRegistry();


    public Histogram scope(String key) {
        return registry.histogram(key);
    }

    public Object getValueAndReset() {
        final Map<String, Number> ret = new HashMap<String, Number>();

        for (Map.Entry<String, Histogram> entry : registry.getHistograms().entrySet()) {
            String prefix = entry.getKey() + "/";
            Histogram histogram = entry.getValue();
            Snapshot snapshot = histogram.getSnapshot();

            ret.put(prefix + "count", histogram.getCount());
            ret.put(prefix + "max", snapshot.getMax());
            ret.put(prefix + "min", snapshot.getMin());
            ret.put(prefix + "stddev", snapshot.getStdDev());
            ret.put(prefix + "p50", snapshot.getMedian());
            ret.put(prefix + "p75", snapshot.get75thPercentile());
            ret.put(prefix + "p95", snapshot.get95thPercentile());
            ret.put(prefix + "p98", snapshot.get98thPercentile());
            ret.put(prefix + "p99", snapshot.get99thPercentile());
            ret.put(prefix + "p999", snapshot.get999thPercentile());

        }
        return ret;
    }
}


