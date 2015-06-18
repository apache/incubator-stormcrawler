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

package com.digitalpebble.storm.crawler.elasticsearch;

import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.digitalpebble.storm.crawler.ConfigurableTopology;
import com.digitalpebble.storm.crawler.Constants;
import com.digitalpebble.storm.crawler.bolt.FetcherBolt;
import com.digitalpebble.storm.crawler.bolt.JSoupParserBolt;
import com.digitalpebble.storm.crawler.bolt.SiteMapParserBolt;
import com.digitalpebble.storm.crawler.bolt.StatusStreamBolt;
import com.digitalpebble.storm.crawler.bolt.URLPartitionerBolt;
import com.digitalpebble.storm.crawler.elasticsearch.bolt.IndexerBolt;
import com.digitalpebble.storm.crawler.elasticsearch.metrics.MetricsConsumer;
import com.digitalpebble.storm.crawler.elasticsearch.persistence.ElasticSearchSpout;
import com.digitalpebble.storm.crawler.elasticsearch.persistence.StatusUpdaterBolt;

/**
 * Dummy topology to play with the spouts and bolts on ElasticSearch
 */
public class ESCrawlTopology extends ConfigurableTopology {

    public static void main(String[] args) throws Exception {
        ConfigurableTopology.start(new ESCrawlTopology(), args);
    }

    @Override
    protected int run(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new ElasticSearchSpout());

        builder.setBolt("partitioner", new URLPartitionerBolt())
                .shuffleGrouping("spout");

        builder.setBolt("fetch", new FetcherBolt()).fieldsGrouping(
                "partitioner", new Fields("key"));

        builder.setBolt("sitemap", new SiteMapParserBolt())
                .localOrShuffleGrouping("fetch");

        builder.setBolt("parse", new JSoupParserBolt()).localOrShuffleGrouping(
                "sitemap");

        // consider that the process has been succesful regardless of what
        // happens with the indexing
        builder.setBolt("switch", new StatusStreamBolt())
                .localOrShuffleGrouping("parse");

        builder.setBolt("indexer", new IndexerBolt()).localOrShuffleGrouping(
                "parse");

        builder.setBolt("status", new StatusUpdaterBolt())
                .localOrShuffleGrouping("switch", Constants.StatusStreamName)
                .localOrShuffleGrouping("fetch", Constants.StatusStreamName)
                .localOrShuffleGrouping("sitemap", Constants.StatusStreamName)
                .localOrShuffleGrouping("parse", Constants.StatusStreamName);

        conf.registerMetricsConsumer(MetricsConsumer.class);

        return submit("crawl", conf, builder);
    }
}
