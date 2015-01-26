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

package com.digitalpebble.storm.crawler.bolt;

import java.util.Map;

import org.apache.commons.lang.StringUtils;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import com.digitalpebble.storm.crawler.util.ConfUtils;

/**
 * A generic bolt for indexing documents which determines which endpoint to use
 * based on the configuration and delegates the indexing to it.
 */
@SuppressWarnings("serial")
public class IndexerBolt extends BaseRichBolt {

    private BaseRichBolt endpoint;

    @Override
    public void prepare(Map conf, TopologyContext context,
            OutputCollector collector) {

        // get the implementation to use
        // and instantiate it
        String className = ConfUtils.getString(conf,
                "stormcrawler.indexer.class");

        if (StringUtils.isBlank(className)) {
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

    @Override
    public void execute(Tuple tuple) {
        if (endpoint != null)
            endpoint.execute(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        if (endpoint != null)
            endpoint.declareOutputFields(declarer);
    }

}