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

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

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
