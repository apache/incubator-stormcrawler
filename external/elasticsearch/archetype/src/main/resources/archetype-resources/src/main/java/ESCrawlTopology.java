#set($symbol_pound='#')#set($symbol_dollar='$')#set($symbol_escape='\')

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

package ${package};

import org.apache.storm.metric.LoggingMetricsConsumer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import org.apache.stormcrawler.ConfigurableTopology;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.bolt.FetcherBolt;
import org.apache.stormcrawler.bolt.JSoupParserBolt;
import org.apache.stormcrawler.bolt.SiteMapParserBolt;
import org.apache.stormcrawler.bolt.URLFilterBolt;
import org.apache.stormcrawler.bolt.URLPartitionerBolt;
import org.apache.stormcrawler.elasticsearch.bolt.DeletionBolt;
import org.apache.stormcrawler.elasticsearch.bolt.IndexerBolt;
import org.apache.stormcrawler.elasticsearch.metrics.MetricsConsumer;
import org.apache.stormcrawler.elasticsearch.metrics.StatusMetricsBolt;
import org.apache.stormcrawler.elasticsearch.persistence.AggregationSpout;
import org.apache.stormcrawler.elasticsearch.persistence.StatusUpdaterBolt;
import org.apache.stormcrawler.spout.FileSpout;
import org.apache.stormcrawler.util.ConfUtils;
import org.apache.stormcrawler.util.URLStreamGrouping;
import org.apache.stormcrawler.tika.ParserBolt;
import org.apache.stormcrawler.tika.RedirectionBolt;

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

		if (args.length == 0) {
			System.err.println("ESCrawlTopology seed_dir file_filter");
			return -1;
		}

		// set to the real number of shards ONLY if es.status.routing is set to
		// true in the configuration
		int numShards = 1;

		builder.setSpout("filespout", new FileSpout(args[0], args[1], true));

		Fields key = new Fields("url");

		builder.setBolt("filter", new URLFilterBolt()).fieldsGrouping("filespout", Constants.StatusStreamName, key);

		builder.setSpout("spout", new AggregationSpout(), numShards);

		builder.setBolt("status_metrics", new StatusMetricsBolt()).shuffleGrouping("spout");

		builder.setBolt("partitioner", new URLPartitionerBolt(), numWorkers).shuffleGrouping("spout");

		builder.setBolt("fetch", new FetcherBolt(), numWorkers).fieldsGrouping("partitioner", new Fields("key"));

		builder.setBolt("sitemap", new SiteMapParserBolt(), numWorkers).localOrShuffleGrouping("fetch");

		builder.setBolt("parse", new JSoupParserBolt(), numWorkers).localOrShuffleGrouping("sitemap");

		builder.setBolt("shunt", new RedirectionBolt()).localOrShuffleGrouping("parse");

		builder.setBolt("tika", new ParserBolt()).localOrShuffleGrouping("shunt", "tika");

		builder.setBolt("indexer", new IndexerBolt(), numWorkers).localOrShuffleGrouping("shunt")
				.localOrShuffleGrouping("tika");

		builder.setBolt("status", new StatusUpdaterBolt(), numWorkers)
				.fieldsGrouping("fetch", Constants.StatusStreamName, key)
				.fieldsGrouping("sitemap", Constants.StatusStreamName, key)
				.fieldsGrouping("parse", Constants.StatusStreamName, key)
				.fieldsGrouping("tika", Constants.StatusStreamName, key)
				.fieldsGrouping("indexer", Constants.StatusStreamName, key)
				.customGrouping("filter", Constants.StatusStreamName, new URLStreamGrouping());

		builder.setBolt("deleter", new DeletionBolt(), numWorkers).localOrShuffleGrouping("status",
				Constants.DELETION_STREAM_NAME);

		conf.registerMetricsConsumer(MetricsConsumer.class);
		conf.registerMetricsConsumer(LoggingMetricsConsumer.class);

		return submit("crawl", conf, builder);
	}
}
