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
package com.digitalpebble.stormcrawler.spout;

import com.digitalpebble.stormcrawler.Constants;
import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.persistence.Status;
import com.digitalpebble.stormcrawler.util.StringTabScheme;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stores URLs in memory. Useful for testing and debugging in local mode or with a single worker.
 * Uses StringTabScheme to parse the lines into URLs and Metadata, generates tuples on the default
 * stream unless withDiscoveredStatus is set to true. Can be used with the MemoryStatusUpdater to
 * receive discovered URLs and emulate a recursive crawl.
 */
public class MemorySpout extends BaseRichSpout {

    private static final Logger LOG = LoggerFactory.getLogger(MemorySpout.class);

    private SpoutOutputCollector _collector;
    private final StringTabScheme scheme = new StringTabScheme();
    private boolean active = true;

    private boolean withDiscoveredStatus = false;

    private static final PriorityQueue<ScheduledURL> queue = new PriorityQueue<>();

    private final String[] startingURLs;

    public MemorySpout(String... urls) {
        this(false, urls);
    }

    /**
     * Emits tuples with DISCOVERED status, which is useful when injecting seeds directly to a
     * statusupdaterbolt.
     *
     * @param withDiscoveredStatus whether the tuples generated should contain a Status field with
     *     DISCOVERED as value and be emitted on the status stream
     */
    public MemorySpout(boolean withDiscoveredStatus, String... urls) {
        this.withDiscoveredStatus = withDiscoveredStatus;
        startingURLs = urls;
    }

    /** Add a new URL with the given metadata and nextFetch-date */
    public static void add(String url, Metadata md, Date nextFetch) {
        LOG.debug("Adding {} with md {} and nextFetch {}", url, md, nextFetch);
        ScheduledURL tuple = new ScheduledURL(url, md, nextFetch);
        synchronized (queue) {
            queue.add(tuple);
        }
    }

    @Override
    public void open(
            Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;

        // check that there is only one instance of it
        int totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
        if (totalTasks > 1) {
            throw new RuntimeException("Can't have more than one instance of the MemorySpout");
        }

        Date now = new Date();
        for (String u : startingURLs) {
            LOG.debug("About to deserialize {} ", u);
            List<Object> tuple =
                    scheme.deserialize(ByteBuffer.wrap(u.getBytes(StandardCharsets.UTF_8)));
            add((String) tuple.get(0), (Metadata) tuple.get(1), now);
        }
        context.registerMetric("queue_size", () -> queue.size(), 10);
    }

    @Override
    public void nextTuple() {
        if (!active) return;

        synchronized (queue) {
            // removes the URL
            ScheduledURL tuple = queue.poll();
            if (tuple == null) return;

            // check whether it is due for fetching
            if (tuple.nextFetchDate.after(new Date())) {
                LOG.debug("Tuple {} not ready for fetching", tuple.URL);

                // put it back and wait
                queue.add(tuple);
                return;
            }

            List<Object> tobs = new LinkedList<>();
            tobs.add(tuple.URL);
            tobs.add(tuple.m);

            if (withDiscoveredStatus) {
                tobs.add(Status.DISCOVERED);
                _collector.emit(Constants.StatusStreamName, tobs, tuple.URL);
            } else {
                _collector.emit(tobs, tuple.URL);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(scheme.getOutputFields());
        if (withDiscoveredStatus) {
            // add status field to output
            List<String> s = scheme.getOutputFields().toList();
            s.add("status");
            declarer.declareStream(Constants.StatusStreamName, new Fields(s));
        }
    }

    @Override
    public void activate() {
        super.activate();
        active = true;
    }

    @Override
    public void deactivate() {
        super.deactivate();
        active = false;
    }
}

class ScheduledURL implements Comparable<ScheduledURL> {
    Date nextFetchDate;
    String URL;
    Metadata m;

    ScheduledURL(String URL, Metadata m, Date nextFetchDate) {
        this.nextFetchDate = nextFetchDate;
        this.URL = URL;
        this.m = m;
    }

    @Override
    public String toString() {
        return URL + "\t" + nextFetchDate;
    }

    @Override
    /** Sort by next fetch date then URl * */
    public int compareTo(ScheduledURL o) {
        // compare the URL
        int compString = URL.compareTo(o.URL);
        if (compString == 0) return 0;

        // compare the date
        int comp = nextFetchDate.compareTo(o.nextFetchDate);
        if (comp != 0) return comp;

        return compString;
    }
}
