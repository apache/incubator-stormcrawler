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

import com.digitalpebble.storm.crawler.ConfigurableTopology;
import com.digitalpebble.storm.crawler.Constants;
import com.digitalpebble.storm.crawler.bolt.JSoupParserBolt;
import com.digitalpebble.storm.crawler.bolt.SimpleFetcherBolt;
import com.digitalpebble.storm.crawler.bolt.SiteMapParserBolt;
import com.digitalpebble.storm.crawler.bolt.URLPartitionerBolt;
import com.digitalpebble.storm.crawler.elasticsearch.bolt.IndexerBolt;
import com.digitalpebble.storm.crawler.elasticsearch.metrics.MetricsConsumer;
import com.digitalpebble.storm.crawler.elasticsearch.persistence.ElasticSearchSpout;
import com.digitalpebble.storm.crawler.elasticsearch.persistence.StatusUpdaterBolt;
import com.digitalpebble.storm.crawler.util.ConfUtils;

import org.apache.storm.metric.LoggingMetricsConsumer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

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

        int numWorkers = ConfUtils.getInt(getConf(), "topology.workers", 1);

        int numFetchers = ConfUtils.getInt(getConf(), "fetcher.threads.number",
                50);

        // set to the real number of shards ONLY if es.status.routing is set to
        // true in the configuration
        int numShards = 1;

        builder.setSpout("spout", new ElasticSearchSpout(), numShards);

        builder.setBolt("partitioner", new URLPartitionerBolt(), numWorkers)
                .shuffleGrouping("spout");

        builder.setBolt("fetch", new SimpleFetcherBolt(),
                numFetchers * numWorkers).fieldsGrouping("partitioner",
                new Fields("key"));

        builder.setBolt("sitemap", new SiteMapParserBolt(), numWorkers)
                .localOrShuffleGrouping("fetch");

        builder.setBolt("parse", new JSoupParserBolt(), numWorkers)
                .localOrShuffleGrouping("sitemap");

        builder.setBolt("indexer", new IndexerBolt(), numWorkers)
                .localOrShuffleGrouping("parse");

        builder.setBolt("status", new StatusUpdaterBolt(), numWorkers)
                .localOrShuffleGrouping("fetch", Constants.StatusStreamName)
                .localOrShuffleGrouping("sitemap", Constants.StatusStreamName)
                .localOrShuffleGrouping("parse", Constants.StatusStreamName)
                .localOrShuffleGrouping("indexer", Constants.StatusStreamName);

        conf.registerMetricsConsumer(MetricsConsumer.class);
        conf.registerMetricsConsumer(LoggingMetricsConsumer.class);

        return submit("crawl", conf, builder);
    }
}
