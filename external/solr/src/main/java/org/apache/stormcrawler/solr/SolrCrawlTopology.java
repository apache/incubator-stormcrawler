/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.stormcrawler.solr;

import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.stormcrawler.ConfigurableTopology;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.bolt.FetcherBolt;
import org.apache.stormcrawler.bolt.JSoupParserBolt;
import org.apache.stormcrawler.bolt.SiteMapParserBolt;
import org.apache.stormcrawler.bolt.URLPartitionerBolt;
import org.apache.stormcrawler.solr.bolt.DeletionBolt;
import org.apache.stormcrawler.solr.bolt.IndexerBolt;
import org.apache.stormcrawler.solr.metrics.MetricsConsumer;
import org.apache.stormcrawler.solr.persistence.SolrSpout;
import org.apache.stormcrawler.solr.persistence.StatusUpdaterBolt;

/** Dummy topology to play with the spouts and bolts on Solr */
public class SolrCrawlTopology extends ConfigurableTopology {

    public static void main(String[] args) throws Exception {
        ConfigurableTopology.start(new SolrCrawlTopology(), args);
    }

    @Override
    protected int run(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new SolrSpout());

        builder.setBolt("partitioner", new URLPartitionerBolt()).shuffleGrouping("spout");

        builder.setBolt("fetch", new FetcherBolt())
                .fieldsGrouping("partitioner", new Fields("key"));

        builder.setBolt("sitemap", new SiteMapParserBolt()).localOrShuffleGrouping("fetch");

        builder.setBolt("parse", new JSoupParserBolt()).localOrShuffleGrouping("sitemap");

        builder.setBolt("indexer", new IndexerBolt()).localOrShuffleGrouping("parse");

        builder.setBolt("status", new StatusUpdaterBolt())
                .localOrShuffleGrouping("fetch", Constants.StatusStreamName)
                .localOrShuffleGrouping("sitemap", Constants.StatusStreamName)
                .localOrShuffleGrouping("parse", Constants.StatusStreamName)
                .localOrShuffleGrouping("indexer", Constants.StatusStreamName);

        builder.setBolt("deleter", new DeletionBolt())
                .localOrShuffleGrouping("status", Constants.DELETION_STREAM_NAME);

        conf.registerMetricsConsumer(MetricsConsumer.class);

        return submit("crawl", conf, builder);
    }
}
