package com.digitalpebble.storm.crawler.bolt.indexing;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import com.digitalpebble.storm.crawler.StormConfiguration;
import com.digitalpebble.storm.crawler.bolt.FetchUrlBolt;

/**
 * A generic bolt for indexing documents which determines which endpoint to use
 * based on the configuration and delegates the indexing to it.
 ***/

@SuppressWarnings("serial")
public class IndexerBolt extends BaseRichBolt {

	private Configuration config;
	private BaseRichBolt endpoint;

	private static final org.slf4j.Logger LOG = LoggerFactory
			.getLogger(IndexerBolt.class);

	public void prepare(Map conf, TopologyContext context,
			OutputCollector collector) {
		config = StormConfiguration.create();

		// get the implementation to use
		// and instanciate it
		String className = config.get("stormcrawler.indexer.class");

		if (className == null) {
			throw new RuntimeException("No configuration found for indexing");
		}

		try {
			final Class<BaseRichBolt> implClass = (Class<BaseRichBolt>) Class
					.forName(className);
			endpoint = implClass.newInstance();
		} catch (final Exception e) {
			throw new RuntimeException("Couldn't create " + className, e);
		}

		if (endpoint != null)
			endpoint.prepare(conf, context, collector);
	}

	public void execute(Tuple tuple) {
		if (endpoint != null)
			endpoint.execute(tuple);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		if (endpoint != null)
			endpoint.declareOutputFields(declarer);
	}

}