/**
 * Licensed to DigitalPebble Ltd under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * DigitalPebble licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.digitalpebble.storm.crawler.metrics;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.metric.api.IMetric;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;

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

        for (Map.Entry<String, Histogram> entry : registry.getHistograms()
                .entrySet()) {
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
