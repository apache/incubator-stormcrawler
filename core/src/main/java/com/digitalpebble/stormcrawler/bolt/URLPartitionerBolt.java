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
package com.digitalpebble.stormcrawler.bolt;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.util.PartitionMode;
import com.digitalpebble.stormcrawler.util.PartitionUtil;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Generates a partition key for a given URL based on the hostname, domain or IP address. */
public class URLPartitionerBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(URLPartitionerBolt.class);

    private OutputCollector _collector;

    private MultiCountMetric eventCounter;

    private Map<String, String> cache;

    private PartitionMode mode;

    @Override
    public void execute(Tuple tuple) {
        String url = tuple.getStringByField("url");

        Metadata metadata;
        if (tuple.contains("metadata")) metadata = (Metadata) tuple.getValueByField("metadata");
        else metadata = Metadata.empty;

        String partitionKey = null;

        if (mode == PartitionMode.QUEUE_MODE_IP) {
            String ip_provided = metadata.getFirstValue("ip");
            // IP in metadata?
            if (StringUtils.isNotBlank(ip_provided)) {
                partitionKey = ip_provided;
                eventCounter.scope("provided").incrBy(1);
            }
        }

        // Either not ip or still not assigned
        if (partitionKey == null) {
            // try to parse
            URL u;
            try {
                u = new URL(url);
            } catch (MalformedURLException e1) {
                eventCounter.scope("Invalid URL").incrBy(1);
                LOG.warn("Invalid URL: {}", url);
                // ack it so that it doesn't get replayed
                _collector.ack(tuple);
                return;
            }

            // TODO: Maybe use the getPartitionKeyByMode without the cache? IP address of host could
            // change over time.
            if (mode == PartitionMode.QUEUE_MODE_IP) {
                String host = u.getHost();
                // try to get it from cache first
                partitionKey = cache.get(host);
                if (partitionKey != null) {
                    eventCounter.scope("from cache").incrBy(1);
                } else {
                    try {
                        final InetAddress addr = InetAddress.getByName(host);
                        partitionKey = addr.getHostAddress();
                        // add to cache
                        cache.put(host, partitionKey);
                    } catch (final Exception e) {
                        eventCounter.scope("Unable to resolve IP").incrBy(1);
                        LOG.warn("Unable to resolve IP for: {}", host);
                        _collector.ack(tuple);
                        return;
                    }
                }
            } else {
                // Always skips the PartitionMode.QUEUE_MODE_IP, but that is okay. We only need to
                // cover the other cases.
                partitionKey = PartitionUtil.getPartitionKeyByMode(u, mode);
            }
        }

        LOG.debug("Partition Key for: {} > {}", url, partitionKey);

        _collector.emit(tuple, new Values(url, partitionKey, metadata));
        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url", "key", "metadata"));
    }

    @Override
    public void prepare(
            Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {

        mode = PartitionUtil.readPartitionModeForPartitioner(stormConf);

        LOG.info("Using partition mode : {}", mode);

        _collector = collector;
        // Register a "MultiCountMetric" to count different events in this bolt
        // Storm will emit the counts every n seconds to a special bolt via a
        // system stream
        // The data can be accessed by registering a "MetricConsumer" in the
        // topology
        this.eventCounter = context.registerMetric("URLPartitioner", new MultiCountMetric(), 10);

        final int MAX_ENTRIES = 500;
        cache =
                new LinkedHashMap<String, String>(MAX_ENTRIES + 1, .75F, true) {
                    // This method is called just after a new entry has been added
                    @Override
                    public boolean removeEldestEntry(Map.Entry eldest) {
                        return size() > MAX_ENTRIES;
                    }
                };

        // If the cache is to be used by multiple threads,
        // the cache must be wrapped with code to synchronize the methods
        cache = Collections.synchronizedMap(cache);
    }
}
