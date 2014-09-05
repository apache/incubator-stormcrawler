/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
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

package com.digitalpebble.storm.crawler.bolt;

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.digitalpebble.storm.crawler.util.Configuration;
import com.digitalpebble.storm.crawler.StormConfiguration;

public class IPResolutionBolt extends BaseRichBolt {

    public static final Logger LOG = LoggerFactory
            .getLogger(IPResolutionBolt.class);

    OutputCollector _collector;
    String groupingScheme;
    public static final Configuration config = StormConfiguration.create();

    public void execute(Tuple tuple) {
        String url = tuple.getStringByField("url");
        HashMap<String, String[]> metadata = null;

        if (tuple.contains("metadata"))
            metadata = (HashMap<String, String[]>) tuple
                    .getValueByField("metadata");

        String ip = null;
        String host = "";
        String groupingParam;

        URL u;
        try {
            u = new URL(url);
            host = u.getHost();
        } catch (MalformedURLException e1) {
            LOG.warn("Invalid URL: " + url);
            // ack it so that it doesn't get replayed
            _collector.ack(tuple);
            return;
        }

        try {
            long start = System.currentTimeMillis();
            final InetAddress addr = InetAddress.getByName(host);
            ip = addr.getHostAddress();
            long end = System.currentTimeMillis();

            LOG.info("IP for: " + host + " > " + ip + " in " + (end - start)
                    + " msec");
            if (groupingScheme.equals("ip")) {
                groupingParam = ip;
            } else {
                groupingParam = "domain";
            }

            _collector.emit(tuple, new Values(url, groupingParam, metadata));
            _collector.ack(tuple);
        } catch (final Exception e) {
            LOG.warn("Unable to resolve IP for: " + host);
            _collector.fail(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        String groupingScheme = config.get("fetcher.grouping.scheme", "domain");
        declarer.declare(new Fields("url", groupingScheme, "metadata"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {
        this.groupingScheme = config.get("fetcher.grouping.scheme", "domain");
        _collector = collector;
    }

}
