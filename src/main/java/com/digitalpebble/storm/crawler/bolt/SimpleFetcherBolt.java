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

import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.digitalpebble.storm.crawler.Metadata;
import com.digitalpebble.storm.crawler.protocol.Protocol;
import com.digitalpebble.storm.crawler.protocol.ProtocolFactory;
import com.digitalpebble.storm.crawler.protocol.ProtocolResponse;

import crawlercommons.robots.BaseRobotRules;

/**
 * A single-threaded fetcher with no internal queue. Use of this fetcher
 * requires that the user implement an external queue that enforces crawl-delay
 * politeness constraints.
 **/

public class SimpleFetcherBolt extends BaseRichBolt {

    public static final Logger LOG = LoggerFactory
            .getLogger(SimpleFetcherBolt.class);

    private Config conf;

    private OutputCollector _collector;

    private MultiCountMetric eventCounter;

    private ProtocolFactory protocolFactory;

    private int taskIndex = -1;

    private void checkConfiguration() {

        // ensure that a value has been set for the agent name and that that
        // agent name is the first value in the agents we advertise for robot
        // rules parsing
        String agentName = (String) getConf().get("http.agent.name");
        if (agentName == null || agentName.trim().length() == 0) {
            String message = "Fetcher: No agents listed in 'http.agent.name'"
                    + " property.";
            if (LOG.isErrorEnabled()) {
                LOG.error(message);
            }
            throw new IllegalArgumentException(message);
        }
    }

    private Config getConf() {
        return this.conf;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {

        _collector = collector;
        this.conf = new Config();
        this.conf.putAll(stormConf);

        checkConfiguration();

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long start = System.currentTimeMillis();
        if (LOG.isInfoEnabled()) {
            LOG.info("[Fetcher #" + taskIndex + "] : starting at "
                    + sdf.format(start));
        }

        // Register a "MultiCountMetric" to count different events in this bolt
        // Storm will emit the counts every n seconds to a special bolt via a
        // system stream
        // The data can be accessed by registering a "MetricConsumer" in the
        // topology
        this.eventCounter = context.registerMetric("fetcher_counter",
                new MultiCountMetric(), 10);

        protocolFactory = new ProtocolFactory(conf);

        this.taskIndex = context.getThisTaskIndex();

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url", "content", "metadata"));
    }

    private boolean isTickTuple(Tuple tuple) {
        String sourceComponent = tuple.getSourceComponent();
        String sourceStreamId = tuple.getSourceStreamId();
        return sourceComponent.equals(Constants.SYSTEM_COMPONENT_ID)
                && sourceStreamId.equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 1);
        return conf;
    }

    @Override
    public void execute(Tuple input) {

        if (isTickTuple(input)) {
            return;
        }

        if (!input.contains("url")) {
            LOG.info("[Fetcher #" + taskIndex + "] Missing field url in tuple "
                    + input);
            // ignore silently
            _collector.ack(input);
            return;
        }

        String urlString = input.getStringByField("url");

        // has one but what about the content?
        if (StringUtils.isBlank(urlString)) {
            LOG.info("[Fetcher #" + taskIndex
                    + "] Missing value for field url in tuple " + input);
            // ignore silently
            _collector.ack(input);
            return;
        }

        URL url;

        try {
            url = new URL(urlString);
        } catch (MalformedURLException e) {
            LOG.error(urlString + " is a malformed URL");
            // ignore silently
            _collector.ack(input);
            return;
        }

        try {
            Protocol protocol = protocolFactory.getProtocol(url);

            BaseRobotRules rules = protocol.getRobotRules(urlString);
            if (!rules.isAllowed(urlString)) {
                if (LOG.isInfoEnabled())
                    LOG.info("Denied by robots.txt: " + urlString);

                // ignore silently
                _collector.ack(input);
                return;
            }

            Metadata metadata = null;
            if (input.contains("metadata")) {
                metadata = (Metadata) input.getValueByField("metadata");
            }
            if (metadata == null) {
                metadata = metadata.empty;
            }

            ProtocolResponse response = protocol.getProtocolOutput(urlString,
                    metadata);

            LOG.info("[Fetcher #" + taskIndex + "] Fetched " + urlString
                    + " with status " + response.getStatusCode());

            eventCounter.scope("fetched").incrBy(1);

            response.getMetadata().setValue("fetch.statusCode",
                    Integer.toString(response.getStatusCode()));

            // update the stats
            // eventStats.scope("KB downloaded").update((long)
            // content.length / 1024l);
            // eventStats.scope("# pages").update(1);

            for (Entry<String, String[]> entry : metadata.getMap().entrySet()) {
                response.getMetadata().setValues(entry.getKey(),
                        entry.getValue());
            }

            _collector.emit(Utils.DEFAULT_STREAM_ID, input, new Values(
                    urlString, response.getContent(), response.getMetadata()));
            _collector.ack(input);

        } catch (Exception exece) {
            if (exece.getCause() instanceof java.util.concurrent.TimeoutException)
                LOG.error("Socket timeout fetching " + urlString);
            else if (exece.getMessage().contains("connection timed out"))
                LOG.error("Socket timeout fetching " + urlString);
            else
                LOG.error("Exception while fetching " + urlString, exece);

            eventCounter.scope("failed").incrBy(1);

            // Don't fail the tuple; this will cause many spouts to replay the
            // tuple,
            // which for requests that continually time out, may choke the
            // topology
            _collector.ack(input);
        }

    }

}
