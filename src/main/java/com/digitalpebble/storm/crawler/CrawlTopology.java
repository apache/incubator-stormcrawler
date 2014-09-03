package com.digitalpebble.storm.crawler;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.digitalpebble.storm.crawler.bolt.IPResolutionBolt;
import com.digitalpebble.storm.crawler.bolt.indexing.IndexerBolt;
import com.digitalpebble.storm.crawler.bolt.parser.ParserBolt;
import com.digitalpebble.storm.crawler.fetcher.Fetcher;
import com.digitalpebble.storm.crawler.spout.RandomURLSpout;
import com.digitalpebble.storm.crawler.util.Configuration;
import com.digitalpebble.storm.crawler.StormConfiguration;

import java.io.IOException;

/**
 * Dummy topology to play with the spouts and bolts
 */
public class CrawlTopology {

    Configuration config;

    public static void main(String[] args) throws Exception {

        CrawlTopology topology = new CrawlTopology();
        topology.open();
        StormTopology stormTopology = topology.buildTopology();

        Config conf = new Config();
        conf.setDebug(true);
        conf.registerMetricsConsumer(DebugMetricConsumer.class);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopology(args[0], conf,
                    stormTopology);
        } else {
            conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("crawl", conf, stormTopology);

            Thread.sleep(10000);

            cluster.shutdown();
        }
    }

    public void open() throws IOException {
        this.config = StormConfiguration.create();
    }

    public StormTopology buildTopology() {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new RandomURLSpout());

        builder.setBolt("ip", new IPResolutionBolt()).shuffleGrouping("spout");

        // Retrieve the fetcher grouping scheme defined in storm-default. Default is domain
        String fetcherGroupingMethod = config.get("fetcher.grouping.scheme", "domain");

        // Attach the fetch bolt to the IP resolution bolt, using the stream grouping specified
        // in the configuration
        builder.setBolt("fetch", new Fetcher()).fieldsGrouping("ip",
                new Fields(fetcherGroupingMethod));

        builder.setBolt("parse", new ParserBolt()).shuffleGrouping("fetch");

        builder.setBolt("index", new IndexerBolt()).shuffleGrouping("parse");
        return builder.createTopology();
    }
}
