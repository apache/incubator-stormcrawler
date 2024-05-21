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
package ${package};

import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.stormcrawler.bolt.FeedParserBolt;
import org.apache.stormcrawler.bolt.FetcherBolt;
import org.apache.stormcrawler.bolt.JSoupParserBolt;
import org.apache.stormcrawler.bolt.SiteMapParserBolt;
import org.apache.stormcrawler.bolt.URLPartitionerBolt;
import org.apache.stormcrawler.indexing.StdOutIndexer;
import org.apache.stormcrawler.tika.ParserBolt;
import org.apache.stormcrawler.tika.RedirectionBolt;
import org.apache.stormcrawler.urlfrontier.Spout;
import org.apache.stormcrawler.urlfrontier.StatusUpdaterBolt;

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

		builder.setSpout("spout", new Spout());

		builder.setBolt("partitioner", new URLPartitionerBolt()).shuffleGrouping("spout");

		builder.setBolt("fetch", new FetcherBolt()).fieldsGrouping("partitioner", new Fields("key"));

		builder.setBolt("sitemap", new SiteMapParserBolt()).localOrShuffleGrouping("fetch");

		builder.setBolt("feeds", new FeedParserBolt()).localOrShuffleGrouping("sitemap");

		builder.setBolt("parse", new JSoupParserBolt()).localOrShuffleGrouping("feeds");

		builder.setBolt("shunt", new RedirectionBolt()).localOrShuffleGrouping("parse");

		builder.setBolt("tika", new ParserBolt()).localOrShuffleGrouping("shunt", "tika");

		builder.setBolt("index", new StdOutIndexer()).localOrShuffleGrouping("shunt").localOrShuffleGrouping("tika");

		Fields furl = new Fields("url");

		// can also use MemoryStatusUpdater for simple recursive crawls
		builder.setBolt("status", new StatusUpdaterBolt()).fieldsGrouping("fetch", Constants.StatusStreamName, furl)
				.fieldsGrouping("sitemap", Constants.StatusStreamName, furl)
				.fieldsGrouping("feeds", Constants.StatusStreamName, furl)
				.fieldsGrouping("parse", Constants.StatusStreamName, furl)
				.fieldsGrouping("tika", Constants.StatusStreamName, furl)
				.fieldsGrouping("index", Constants.StatusStreamName, furl);

		return submit("crawl", conf, builder);
	}
}
