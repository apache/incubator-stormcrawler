package com.digitalpebble.storm.crawler;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.digitalpebble.storm.crawler.bolt.FetchUrlBolt;
import com.digitalpebble.storm.crawler.bolt.IPResolutionBolt;
import com.digitalpebble.storm.crawler.bolt.indexing.IndexerBolt;
import com.digitalpebble.storm.crawler.bolt.indexing.PrinterBolt;
import com.digitalpebble.storm.crawler.bolt.parser.ParserBolt;
import com.digitalpebble.storm.crawler.spout.RandomURLSpout;

/**
 * Dummy topology to play with the spouts and bolts
 */
public class CrawlTopology {

	public static void main(String[] args) throws Exception {

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout", new RandomURLSpout());

		builder.setBolt("ip", new IPResolutionBolt()).shuffleGrouping("spout");

		builder.setBolt("fetch", new FetchUrlBolt()).fieldsGrouping("ip",
				new Fields("ip"));

		builder.setBolt("parse", new ParserBolt()).shuffleGrouping("fetch");
		
		builder.setBolt("index", new IndexerBolt()).shuffleGrouping("parse");


		Config conf = new Config();
		conf.setDebug(true);
        conf.registerMetricsConsumer(DebugMetricConsumer.class);

		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);

			StormSubmitter.submitTopology(args[0], conf,
					builder.createTopology());
		} else {
			conf.setMaxTaskParallelism(3);

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("crawl", conf, builder.createTopology());

			Thread.sleep(10000);

			cluster.shutdown();
		}
	}
}
