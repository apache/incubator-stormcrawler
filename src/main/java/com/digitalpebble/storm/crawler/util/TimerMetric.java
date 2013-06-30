package com.digitalpebble.storm.crawler.util;

import backtype.storm.metric.api.IMetric;
import backtype.storm.metric.api.IReducer;
import backtype.storm.metric.api.ReducedMetric;
import com.codahale.metrics.*;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;


/**
 * @author Enno Shioji (enno.shioji@peerindex.com)
 */
public class TimerMetric implements IMetric {
    private final MetricRegistry registry = new MetricRegistry();


    public Timer scope(String key) {
        return registry.timer(key);
    }

    public Object getValueAndReset() {
        final Map<String, Number> ret = new HashMap<String, Number>();

        for (Map.Entry<String, Timer> entry : registry.getTimers().entrySet()) {
            String prefix = entry.getKey() + "/";
            Timer timer = entry.getValue();
            Snapshot snapshot = timer.getSnapshot();

            ret.put(prefix + "count", timer.getCount());
            ret.put(prefix + "max", snapshot.getMax());
            ret.put(prefix + "mean", snapshot.getMean());
            ret.put(prefix + "min", snapshot.getMin());
            ret.put(prefix + "stddev", snapshot.getStdDev());
            ret.put(prefix + "p50", snapshot.getMedian());
            ret.put(prefix + "p75", snapshot.get75thPercentile());
            ret.put(prefix + "p95", snapshot.get95thPercentile());
            ret.put(prefix + "p98", snapshot.get98thPercentile());
            ret.put(prefix + "p99", snapshot.get99thPercentile());
            ret.put(prefix + "p999", snapshot.get999thPercentile());
            ret.put(prefix + "mean_rate", timer.getMeanRate());
            ret.put(prefix + "m1_rate", timer.getOneMinuteRate());
            ret.put(prefix + "m5_rate", timer.getFiveMinuteRate());
            ret.put(prefix + "m15_rate", timer.getFifteenMinuteRate());
        }
        return ret;
    }
}
