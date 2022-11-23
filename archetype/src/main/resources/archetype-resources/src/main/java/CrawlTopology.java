package ${package};

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

import com.digitalpebble.stormcrawler.*;
import com.digitalpebble.stormcrawler.bolt.FetcherBolt;
import com.digitalpebble.stormcrawler.bolt.JSoupParserBolt;
import com.digitalpebble.stormcrawler.bolt.SiteMapParserBolt;
import com.digitalpebble.stormcrawler.bolt.URLPartitionerBolt;
import com.digitalpebble.stormcrawler.bolt.FeedParserBolt;
import com.digitalpebble.stormcrawler.indexing.StdOutIndexer;
import com.digitalpebble.stormcrawler.persistence.StdOutStatusUpdater;
import com.digitalpebble.stormcrawler.urlfrontier.Spout;
import com.digitalpebble.stormcrawler.urlfrontier.StatusUpdaterBolt;
import com.digitalpebble.stormcrawler.tika.ParserBolt;
import com.digitalpebble.stormcrawler.tika.RedirectionBolt;

import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

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
