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
public class MeterMetric implements IMetric{
    private static final Logger log = LoggerFactory.getLogger(MeterMetric.class);

    private final MetricRegistry registry = new MetricRegistry();


    public Meter scope(String key) {
        return registry.meter(key);
    }

    public Object getValueAndReset() {
        final Map<String, Number> ret = new HashMap<String, Number>();

        for (Map.Entry<String, Meter> entry : registry.getMeters().entrySet()) {
            String prefix = entry.getKey() + "/";
            Meter meter = entry.getValue();

            ret.put(prefix + "count", meter.getCount());
            ret.put(prefix + "mean_rate", meter.getMeanRate());
            ret.put(prefix + "m1_rate", meter.getOneMinuteRate());
            ret.put(prefix + "m5_rate", meter.getFiveMinuteRate());
            ret.put(prefix + "m15_rate", meter.getFifteenMinuteRate());
        }
        return ret;
    }
}

