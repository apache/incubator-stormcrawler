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

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Map;

import backtype.storm.metric.api.MeanReducer;
import backtype.storm.metric.api.MultiReducedMetric;
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
import com.digitalpebble.storm.crawler.filtering.URLFilters;
import com.digitalpebble.storm.crawler.persistence.Status;
import com.digitalpebble.storm.crawler.protocol.HttpHeaders;
import com.digitalpebble.storm.crawler.protocol.Protocol;
import com.digitalpebble.storm.crawler.protocol.ProtocolFactory;
import com.digitalpebble.storm.crawler.protocol.ProtocolResponse;
import com.digitalpebble.storm.crawler.util.ConfUtils;
import com.digitalpebble.storm.crawler.util.MetadataTransfer;
import com.digitalpebble.storm.crawler.util.URLUtil;

import crawlercommons.robots.BaseRobotRules;

/**
 * A single-threaded fetcher with no internal queue. Use of this fetcher
 * requires that the user implement an external queue that enforces crawl-delay
 * politeness constraints.
 */
public class SimpleFetcherBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory
            .getLogger(SimpleFetcherBolt.class);

    private Config conf;

    private OutputCollector _collector;

    private MultiCountMetric eventCounter;
    private MultiReducedMetric averagedMetrics;

    private ProtocolFactory protocolFactory;

    private URLFilters urlFilters;

    private MetadataTransfer metadataTransfer;

    private int taskIndex = -1;

    private boolean allowRedirs;

    private void checkConfiguration() {

        // ensure that a value has been set for the agent name and that that
        // agent name is the first value in the agents we advertise for robot
        // rules parsing
        String agentName = (String) getConf().get("http.agent.name");
        if (agentName == null || agentName.trim().length() == 0) {
            String message = "Fetcher: No agents listed in 'http.agent.name'"
                    + " property.";
            LOG.error(message);
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

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss",
                Locale.ENGLISH);
        long start = System.currentTimeMillis();
        LOG.info("[Fetcher #{}] : starting at {}", taskIndex, sdf.format(start));

        // Register a "MultiCountMetric" to count different events in this bolt
        // Storm will emit the counts every n seconds to a special bolt via a
        // system stream
        // The data can be accessed by registering a "MetricConsumer" in the
        // topology
        this.eventCounter = context.registerMetric("fetcher_counter",
                new MultiCountMetric(), 10);

        this.averagedMetrics = context.registerMetric("fetcher_average",
                new MultiReducedMetric(new MeanReducer()), 10);

        protocolFactory = new ProtocolFactory(conf);

        this.taskIndex = context.getThisTaskIndex();

        String urlconfigfile = ConfUtils.getString(conf,
                "urlfilters.config.file", "urlfilters.json");

        if (urlconfigfile != null)
            try {
                urlFilters = new URLFilters(conf, urlconfigfile);
            } catch (IOException e) {
                LOG.error("Exception caught while loading the URLFilters");
                throw new RuntimeException(
                        "Exception caught while loading the URLFilters", e);
            }

        metadataTransfer = MetadataTransfer.getInstance(stormConf);

        allowRedirs = ConfUtils.getBoolean(stormConf,
                com.digitalpebble.storm.crawler.Constants.AllowRedirParamName,
                true);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url", "content", "metadata"));
        declarer.declareStream(
                com.digitalpebble.storm.crawler.Constants.StatusStreamName,
                new Fields("url", "metadata", "status"));
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
            LOG.info("[Fetcher #{}] Missing field url in tuple {}", taskIndex,
                    input);
            // ignore silently
            _collector.ack(input);
            return;
        }

        String urlString = input.getStringByField("url");

        // has one but what about the content?
        if (StringUtils.isBlank(urlString)) {
            LOG.info("[Fetcher #{}] Missing value for field url in tuple {}",
                    taskIndex, input);
            // ignore silently
            _collector.ack(input);
            return;
        }

        Metadata metadata = null;

        if (input.contains("metadata"))
            metadata = (Metadata) input.getValueByField("metadata");
        if (metadata == null)
            metadata = Metadata.empty;

        URL url;

        try {
            url = new URL(urlString);
        } catch (MalformedURLException e) {
            LOG.error("{} is a malformed URL", urlString);
            // ignore silently
            _collector.ack(input);
            return;
        }

        try {
            Protocol protocol = protocolFactory.getProtocol(url);

            BaseRobotRules rules = protocol.getRobotRules(urlString);
            if (!rules.isAllowed(urlString)) {
                LOG.info("Denied by robots.txt: {}", urlString);

                // Report to status stream and ack
                _collector
                        .emit(com.digitalpebble.storm.crawler.Constants.StatusStreamName,
                                input, new Values(urlString, metadata,
                                        Status.ERROR));
                _collector.ack(input);
                return;
            }

            long start = System.currentTimeMillis();
            ProtocolResponse response = protocol.getProtocolOutput(urlString,
                    metadata);

            averagedMetrics.scope("fetch_time").update(System.currentTimeMillis() - start);
            averagedMetrics.scope("bytes_fetched").update(response.getContent().length);

            LOG.info("[Fetcher #{}] Fetched {} with status {}", taskIndex,
                    urlString, response.getStatusCode());

            eventCounter.scope("fetched").incrBy(1);

            response.getMetadata().setValue("fetch.statusCode",
                    Integer.toString(response.getStatusCode()));

            // update the stats
            // eventStats.scope("KB downloaded").update((long)
            // content.length / 1024l);
            // eventStats.scope("# pages").update(1);

            response.getMetadata().putAll(metadata);

            // determine the status based on the status code
            Status status = Status.fromHTTPCode(response.getStatusCode());

            // if the status is OK emit on default stream
            if (status.equals(Status.FETCHED)) {
                _collector.emit(
                        Utils.DEFAULT_STREAM_ID,
                        input,
                        new Values(urlString, response.getContent(), response
                                .getMetadata()));
            } else if (status.equals(Status.REDIRECTION)) {
                // Mark URL as redirected
                _collector
                        .emit(com.digitalpebble.storm.crawler.Constants.StatusStreamName,
                                input,
                                new Values(urlString, response.getMetadata(),
                                        status));

                // find the URL it redirects to
                String redirection = response.getMetadata().getFirstValue(
                        HttpHeaders.LOCATION);

                if (allowRedirs && redirection != null
                        && StringUtils.isNotBlank(redirection)) {
                    handleRedirect(input, urlString, redirection,
                            response.getMetadata());
                }
            } else {
                // Error
                _collector
                        .emit(com.digitalpebble.storm.crawler.Constants.StatusStreamName,
                                input,
                                new Values(urlString, response.getMetadata(),
                                        status));
            }

        } catch (Exception exece) {

            String message = exece.getMessage();
            if (message == null)
                message = "";

            if (exece.getCause() instanceof java.util.concurrent.TimeoutException)
                LOG.error("Socket timeout fetching {}", urlString);
            else if (exece.getMessage().contains("connection timed out"))
                LOG.error("Socket timeout fetching {}", urlString);
            else
                LOG.error("Exception while fetching {}", urlString, exece);

            eventCounter.scope("failed").incrBy(1);

            // could be an empty, immutable Metadata
            if (metadata.size() == 0) {
                metadata = new Metadata();
            }

            // add the reason of the failure in the metadata
            metadata.setValue("fetch.exception", message);

            _collector.emit(
                    com.digitalpebble.storm.crawler.Constants.StatusStreamName,
                    input, new Values(urlString, metadata, Status.FETCH_ERROR));
        }

        _collector.ack(input);

    }

    private void handleRedirect(Tuple t, String sourceUrl, String newUrl,
            Metadata sourceMetadata) {
        // build an absolute URL
        URL sURL;
        try {
            sURL = new URL(sourceUrl);
            URL tmpURL = URLUtil.resolveURL(sURL, newUrl);
            newUrl = tmpURL.toExternalForm();
        } catch (MalformedURLException e) {
            LOG.debug("MalformedURLException on {} or {}: {}", sourceUrl,
                    newUrl, e);
            return;
        }

        // apply URL filters
        if (this.urlFilters != null) {
            newUrl = this.urlFilters.filter(sURL, sourceMetadata, newUrl);
        }

        // filtered
        if (newUrl == null) {
            return;
        }

        Metadata metadata = metadataTransfer.getMetaForOutlink(newUrl,
                sourceUrl, sourceMetadata);

        // TODO check that hasn't exceeded max number of redirections

        _collector.emit(
                com.digitalpebble.storm.crawler.Constants.StatusStreamName, t,
                new Values(newUrl, metadata, Status.DISCOVERED));
    }

}
