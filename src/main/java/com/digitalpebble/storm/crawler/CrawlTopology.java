package com.digitalpebble.storm.crawler;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

import com.digitalpebble.storm.crawler.bolt.FetchUrlBolt;
import com.digitalpebble.storm.crawler.bolt.IPResolutionBolt;
import com.digitalpebble.storm.crawler.bolt.PrinterBolt;
import com.digitalpebble.storm.crawler.spout.RandomURLSpout;

/**
 * This topology demonstrates Storm's stream groupings and multilang
 * capabilities.
 */
public class CrawlTopology {

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new RandomURLSpout(), 1);

        builder.setBolt("ip", new IPResolutionBolt(), 1).shuffleGrouping(
                "spout");

        // builder.setBolt("fetch", new FetchUrlBolt(), 12).fieldsGrouping("ip",
        // new Fields("word"));

        builder.setBolt("fetch", new FetchUrlBolt(), 2).shuffleGrouping("ip");
        
        builder.setBolt("print", new PrinterBolt(), 1).shuffleGrouping("fetch");

        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopology(args[0], conf,
                    builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(3);
             conf.put(Config.TOPOLOGY_DEBUG, true);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("crawl", conf, builder.createTopology());

            Thread.sleep(10000);

            cluster.shutdown();
        }
    }
}
