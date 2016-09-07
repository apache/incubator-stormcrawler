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

import java.util.Date;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.persistence.AbstractStatusUpdaterBolt;
import com.digitalpebble.stormcrawler.persistence.Status;
import com.digitalpebble.stormcrawler.util.URLPartitioner;

import redis.clients.jedis.Jedis;

/**
 * TODO use connection pool + batch calls + use transactions?
 **/

public class StatusUpdaterBolt extends AbstractStatusUpdaterBolt {

    private Jedis client;

    private URLPartitioner partitioner;

    @Override
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {

        super.prepare(stormConf, context, collector);

        // connect to Redis
        client = JedisFactory.getClient(stormConf);

        // how to partition
        partitioner = new URLPartitioner();
        partitioner.configure(stormConf);
    }

    @Override
    protected void store(String url, Status status, Metadata metadata,
            Date nextFetch) throws Exception {

        // String representation of metadata with one K/V per line
        String s_metadata = metadata.toString("");

        if (status.equals(Status.DISCOVERED)) {
            // discovered - is it one we already know?
            // check redis for url with 's_' prefix
            // if it does not exist then put it there

            long set = client.setnx("s_" + url, s_metadata);
            if (set == 0) {
                return;
            }

            // and onto the queues as well
            // with prefix 'q_'
            // TODO add sharding
            String partitionKey = partitioner.getPartition(url, metadata);
            client.rpush("q_" + partitionKey, url);
            return;
        }

        // anything else? update the status
        // don't remove from queue - this is handled by the spout
        client.set("s_" + url, s_metadata);

    }

}
