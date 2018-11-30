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

package com.digitalpebble.stormcrawler.elasticsearch;

import org.apache.storm.spout.Scheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import com.digitalpebble.stormcrawler.ConfigurableTopology;
import com.digitalpebble.stormcrawler.bolt.URLFilterBolt;
import com.digitalpebble.stormcrawler.elasticsearch.persistence.StatusUpdaterBolt;
import com.digitalpebble.stormcrawler.persistence.Status;
import com.digitalpebble.stormcrawler.spout.FileSpout;
import com.digitalpebble.stormcrawler.util.StringTabScheme;
import com.digitalpebble.stormcrawler.util.URLStreamGrouping;

/**
 * Topology which reads from files containing seed URLs and distributes to an ES
 * index. Used in local mode to bootstrap a crawl.
 */
public class ESSeedInjector extends ConfigurableTopology {

    public static void main(String[] args) throws Exception {
        ConfigurableTopology.start(new ESSeedInjector(), args);
    }

    @Override
    public int run(String[] args) {

        if (args.length == 0) {
            System.err.println("ESSeedInjector seed_dir file_filter");
            return -1;
        }

        conf.setDebug(false);

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new FileSpout(args[0], args[1], true));

        Fields key = new Fields("url");

        builder.setBolt("filter", new URLFilterBolt()).fieldsGrouping("spout",
                key);

        // example of using the custom URLStreamGrouping
        builder.setBolt("enqueue", new StatusUpdaterBolt(), 10).customGrouping(
                "filter", new URLStreamGrouping());

        return submit("ESSeedInjector", conf, builder);
    }
}