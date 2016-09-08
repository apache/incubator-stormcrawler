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

package com.digitalpebble.stormcrawler.redis;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseComponent;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.util.ConfUtils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

// TODO handle sharding
public class RedisSpout extends BaseComponent implements IRichSpout {

    private final Logger LOG = LoggerFactory.getLogger(getClass());

    private Jedis client;
    private SpoutOutputCollector _collector;

    protected Queue<Values> buffer = new LinkedList<>();

    private boolean active = true;

    private String lastCursor = "0";
    private int maxBucketNum = 10;
    private int maxURLsPerBucket = 1;

    private Map<String, String> mapping = new HashMap<>();

    private static final String StatusMaxBucketParamName = "redis.status.max.buckets";
    private static final String StatusMaxURLsParamName = "redis.status.max.urls.per.bucket";

    @Override
    public void nextTuple() {
        // inactive?
        if (active == false)
            return;

        // have anything in the buffer?
        if (!buffer.isEmpty()) {
            Values fields = buffer.remove();
            String url = fields.get(0).toString();
            this._collector.emit(fields, url);
            return;
        }
        // re-populate the buffer
        populateBuffer();
    }

    private void populateBuffer() {
        Set<String> emptyQueues = new HashSet<>();
        // scan for queues
        ScanParams params = new ScanParams();
        params.match("q_*");
        params.count(maxBucketNum * 2);
        ScanResult<String> result = client.scan(lastCursor, params);

        LOG.debug("Populating buffer with cursor {}", lastCursor);

        // iterate on the hosts / domains / IPs
        int nonEmptyBucket = 0;
        String previousCursor = lastCursor;
        lastCursor = result.getStringCursor();

        for (String key : result.getResult()) {
            // LPOP to get next URL(s) from that domain
            for (int i = 0; i < maxURLsPerBucket; i++) {
                String val = client.lpop(key);
                if (val == null) {
                    emptyQueues.add(key);
                } else {
                    nonEmptyBucket++;
                    // TODO get metadata from status index?
                    buffer.add(new Values(val, new Metadata()));
                    mapping.put(val, key);
                }
            }

            if (nonEmptyBucket == maxBucketNum) {
                // keep same cursor as we haven't quite finished yet
                lastCursor = previousCursor;
                break;
            }
        }

        if (emptyQueues.size() > 0) {
            client.del((String[]) emptyQueues
                    .toArray(new String[emptyQueues.size()]));
        }

        LOG.debug("New cursor {}, {} URLs from {} buckets", lastCursor,
                emptyQueues.size(), nonEmptyBucket);

        // Shuffle the URLs so that we don't get blocks of URLs from the same
        // host or domain
        Collections.shuffle((List) buffer);
    }

    @Override
    public void open(Map stormConf, TopologyContext context,
            SpoutOutputCollector collector) {
        _collector = collector;
        client = JedisFactory.getClient(stormConf);

        maxURLsPerBucket = ConfUtils.getInt(stormConf, StatusMaxURLsParamName,
                1);
        maxBucketNum = ConfUtils.getInt(stormConf, StatusMaxBucketParamName,
                10);

        // check that we can have only 1 instance of the spout
        // at least until we implement sharding
        int totalTasks = context.getComponentTasks(context.getThisComponentId())
                .size();
        if (totalTasks > 1) {
            throw new RuntimeException(
                    "Number of redis spout instances should be 1 but is "
                            + totalTasks);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url", "metadata"));
    }

    @Override
    public void activate() {
        active = true;
    }

    @Override
    public void deactivate() {
        active = false;
    }

    @Override
    public void close() {
        client.close();
    }

    @Override
    public void ack(Object msgId) {
        // remove mapping from URL to key/val
        mapping.remove(msgId);
    }

    @Override
    public void fail(Object msgId) {
        // put URL back into the queue
        String key = mapping.remove(msgId);
        client.rpush(key, msgId.toString());
    }
}
