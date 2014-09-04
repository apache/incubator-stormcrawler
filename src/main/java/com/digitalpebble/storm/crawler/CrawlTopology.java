/*
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

package com.digitalpebble.storm.crawler;

import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.digitalpebble.storm.crawler.bolt.FetcherBolt;
import com.digitalpebble.storm.crawler.bolt.IPResolutionBolt;
import com.digitalpebble.storm.crawler.bolt.indexing.IndexerBolt;
import com.digitalpebble.storm.crawler.bolt.parser.ParserBolt;
import com.digitalpebble.storm.crawler.spout.RandomURLSpout;

/**
 * Dummy topology to play with the spouts and bolts
 */
public class CrawlTopology extends ConfigurableTopology {

    public static void main(String[] args) throws Exception {
        ConfigurableTopology.start(new CrawlTopology(), args);
    }

    @Override
    protected int run(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new RandomURLSpout());

        builder.setBolt("ip", new IPResolutionBolt()).shuffleGrouping("spout");

        builder.setBolt("fetch", new FetcherBolt()).fieldsGrouping("ip",
                new Fields("ip"));

        builder.setBolt("parse", new ParserBolt()).shuffleGrouping("fetch");

        builder.setBolt("index", new IndexerBolt()).shuffleGrouping("parse");

        conf.registerMetricsConsumer(DebugMetricConsumer.class);

        return submit("crawl", conf, builder);
    }

}
