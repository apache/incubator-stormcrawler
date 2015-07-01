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

package com.digitalpebble.storm.crawler.solr;

import com.digitalpebble.storm.crawler.ConfigurableTopology;
import com.digitalpebble.storm.crawler.persistence.Status;
import com.digitalpebble.storm.crawler.solr.persistence.StatusUpdaterBolt;
import com.digitalpebble.storm.crawler.spout.FileSpout;
import com.digitalpebble.storm.crawler.util.StringTabScheme;

import backtype.storm.spout.Scheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * Topology which reads from a file containing seeds and distributes to SQS
 * queues based on the IP / hostname / domain of the URLs. Used in local mode to
 * boostrap a crawl.
 */
public class SeedInjector extends ConfigurableTopology {

	public static void main(String[] args) throws Exception {
		ConfigurableTopology.start(new SeedInjector(), args);
	}

	@Override
	public int run(String[] args) {

		if (args.length == 0) {
			System.err.println("SeedInjector seed_dir file_filter");
			return -1;
		}

		conf.setDebug(false);

		TopologyBuilder builder = new TopologyBuilder();

		Scheme scheme = new StringTabScheme(Status.DISCOVERED);

		builder.setSpout("spout", new FileSpout(args[0], args[1], scheme));

		Fields key = new Fields("url");

		builder.setBolt("enqueue", new StatusUpdaterBolt()).fieldsGrouping("spout", key);

		return submit("SeedInjector", conf, builder);
	}
}