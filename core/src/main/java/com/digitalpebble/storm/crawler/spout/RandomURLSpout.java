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

package com.digitalpebble.storm.crawler.spout;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.utils.Utils;

import com.digitalpebble.storm.crawler.util.StringTabScheme;

/**
 * Produces URLs taken randomly from a finite set. Useful for testing.
 */
public class RandomURLSpout extends BaseRichSpout {
    private SpoutOutputCollector _collector;
    private Random _rand;
    private StringTabScheme scheme = new StringTabScheme();
    private boolean active = true;

    private String[] urls = new String[] { "http://www.lequipe.fr/",
            "http://www.lemonde.fr/", "http://www.bbc.co.uk/",
            "http://www.facebook.com/", "http://www.rmc.fr" };

    public RandomURLSpout(String... urls) {
        this.urls = urls;
    }

    public RandomURLSpout() {
    }

    @Override
    public void open(Map conf, TopologyContext context,
            SpoutOutputCollector collector) {
        _collector = collector;
        _rand = new Random();
    }

    @Override
    public void nextTuple() {
        if (!active)
            return;
        Utils.sleep(100);
        String url = urls[_rand.nextInt(urls.length)];
        _collector.emit(
                scheme.deserialize(url.getBytes(StandardCharsets.UTF_8)), url);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(scheme.getOutputFields());
    }

    @Override
    public void activate() {
        super.activate();
        active = true;
    }

    @Override
    public void deactivate() {
        super.deactivate();
        active = false;
    }

}
