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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.tika.Tika;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.Link;
import org.apache.tika.sax.LinkContentHandler;
import org.apache.tika.sax.TeeContentHandler;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.codahale.metrics.Timer;
import com.digitalpebble.storm.crawler.filtering.URLFilters;
import com.digitalpebble.storm.crawler.util.ConfUtils;
import com.digitalpebble.storm.crawler.util.URLUtil;
import com.digitalpebble.storm.metrics.HistogramMetric;
import com.digitalpebble.storm.metrics.MeterMetric;
import com.digitalpebble.storm.metrics.TimerMetric;

/**
 * Uses Tika to parse the output of a fetch and extract text + metadata
 ***/

@SuppressWarnings("serial")
public class ParserBolt extends BaseRichBolt {

    private Tika tika;

    private URLFilters urlFilters = null;

    private OutputCollector collector;

    private static final org.slf4j.Logger LOG = LoggerFactory
            .getLogger(ParserBolt.class);

    private MeterMetric eventMeters;
    private HistogramMetric eventHistograms;
    private TimerMetric eventTimers;

    private boolean ignoreOutsideHost = false;
    private boolean ignoreOutsideDomain = false;

    public void prepare(Map conf, TopologyContext context,
            OutputCollector collector) {

        String urlconfigfile = ConfUtils.getString(conf,
                "urlfilters.config.file", "urlfilters.json");

        if (urlconfigfile != null)
            try {
                urlFilters = new URLFilters(urlconfigfile);
            } catch (IOException e) {
                LOG.error("Exception caught while loading the URLFilters");
                throw new RuntimeException(
                        "Exception caught while loading the URLFilters", e);
            }

        ignoreOutsideHost = ConfUtils.getBoolean(conf,
                "parser.ignore.outlinks.outside.host", false);
        ignoreOutsideDomain = ConfUtils.getBoolean(conf,
                "parser.ignore.outlinks.outside.domain", false);

        // instanciate Tika
        long start = System.currentTimeMillis();
        tika = new Tika();
        long end = System.currentTimeMillis();

        LOG.debug("Tika loaded in " + (end - start) + " msec");

        this.collector = collector;

        this.eventMeters = context.registerMetric("parser-meter",
                new MeterMetric(), 5);
        this.eventTimers = context.registerMetric("parser-timer",
                new TimerMetric(), 5);
        this.eventHistograms = context.registerMetric("parser-histograms",
                new HistogramMetric(), 5);

    }

    public void execute(Tuple tuple) {
        eventMeters.scope("tuple_in").mark();

        byte[] content = tuple.getBinaryByField("content");
        eventHistograms.scope("content_bytes").update(content.length);

        String url = tuple.getStringByField("url");
        HashMap<String, String[]> metadata = (HashMap<String, String[]>) tuple
                .getValueByField("metadata");

        // TODO check status etc...

        long start = System.currentTimeMillis();

        // rely on mime-type provided by server or guess?

        ByteArrayInputStream bais = new ByteArrayInputStream(content);
        Metadata md = new Metadata();

        String text = null;

        LinkContentHandler linkHandler = new LinkContentHandler();
        ContentHandler textHandler = new BodyContentHandler();
        TeeContentHandler teeHandler = new TeeContentHandler(linkHandler,
                textHandler);
        ParseContext parseContext = new ParseContext();
        // parse
        try {
            tika.getParser().parse(bais, teeHandler, md, parseContext);
            text = textHandler.toString();
        } catch (Exception e) {
            LOG.error("Exception while parsing " + url, e.getMessage());
            eventMeters.scope(
                    "error_content_parsing_" + e.getClass().getSimpleName())
                    .mark();
            collector.fail(tuple);
            eventMeters.scope("tuple_fail").mark();
            return;
        } finally {
            try {
                bais.close();
            } catch (IOException e) {
                LOG.error("Exception while closing stream", e);
            }
        }

        long duration = System.currentTimeMillis() - start;

        LOG.info("Parsed " + url + " in " + duration + " msec");

        // get the outlinks and convert them to strings (for now)
        String fromHost;
        URL url_;
        try {
            url_ = new URL(url);
            fromHost = url_.getHost().toLowerCase();
        } catch (MalformedURLException e1) {
            // we would have known by now as previous
            // components check whether the URL is valid
            LOG.error("MalformedURLException on " + url);
            eventMeters.scope(
                    "error_outlinks_parsing_" + e1.getClass().getSimpleName())
                    .mark();
            collector.fail(tuple);
            eventMeters.scope("tuple_fail").mark();
            return;
        }

        List<Link> links = linkHandler.getLinks();
        Set<String> slinks = new HashSet<String>(links.size());
        for (Link l : links) {
            if (StringUtils.isBlank(l.getUri()))
                continue;
            String urlOL = null;
            try {
                URL tmpURL = URLUtil.resolveURL(url_, l.getUri());
                urlOL = tmpURL.toExternalForm();
            } catch (MalformedURLException e) {
                LOG.debug("MalformedURLException on " + l.getUri());
                eventMeters.scope(
                        "error_out_link_parsing_"
                                + e.getClass().getSimpleName()).mark();
                continue;
            }

            // filter the urls
            if (urlFilters != null) {
                urlOL = urlFilters.filter(urlOL);
                if (urlOL == null) {
                    eventMeters.scope("outlink_filtered").mark();
                    continue;
                }
            }

            if (urlOL != null && ignoreOutsideHost) {
                String toHost;
                try {
                    toHost = new URL(urlOL).getHost().toLowerCase();
                } catch (MalformedURLException e) {
                    toHost = null;
                }
                if (toHost == null || !toHost.equals(fromHost)) {
                    urlOL = null; // skip it
                    eventMeters.scope("outlink_outsideHost").mark();
                    continue;
                }
            }

            if (urlOL != null) {
                slinks.add(urlOL);
                eventMeters.scope("outlink_kept").mark();
            }
        }

        // add parse md to metadata
        for (String k : md.names()) {
            // TODO handle mutliple values
            String[] values = md.getValues(k);
            metadata.put("parse." + k, values);
        }

        collector.emit(tuple, new Values(url, content, metadata, text.trim(),
                slinks));
        collector.ack(tuple);
        eventMeters.scope("tuple_success").mark();
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // output of this module is the list of fields to index
        // with at least the URL, text content

        declarer.declare(new Fields("url", "content", "metadata", "text",
                "outlinks"));
    }

}