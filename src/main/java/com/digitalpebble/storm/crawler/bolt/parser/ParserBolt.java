package com.digitalpebble.storm.crawler.bolt.parser;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
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

import backtype.storm.metric.api.MeanReducer;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.metric.api.MultiReducedMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.digitalpebble.storm.crawler.StormConfiguration;
import com.digitalpebble.storm.crawler.filtering.URLFilters;
import com.digitalpebble.storm.crawler.util.Configuration;
import com.digitalpebble.storm.crawler.util.URLUtil;

/**
 * Uses Tika to parse the output of a fetch and extract text + metadata
 ***/

@SuppressWarnings("serial")
public class ParserBolt extends BaseRichBolt {

    private Configuration config;

    private Tika tika;

    private URLFilters filters = null;

    private OutputCollector collector;

    private static final org.slf4j.Logger LOG = LoggerFactory
            .getLogger(ParserBolt.class);

    private MultiCountMetric eventCounter;
    private MultiReducedMetric eventStats;

    private boolean ignoreOutsideHost = false;
    private boolean ignoreOutsideDomain = false;

    public void prepare(Map conf, TopologyContext context,
            OutputCollector collector) {
        config = StormConfiguration.create();

        String urlconfigfile = config.get("urlfilters.config.file",
                "urlfilters.json");
        if (urlconfigfile != null)
            try {
                filters = new URLFilters(urlconfigfile);
            } catch (IOException e) {
                LOG.error("Exception caught while loading the URLFilters");
            }

        ignoreOutsideHost = config.getBoolean(
                "parser.ignore.outlinks.outside.host", false);
        ignoreOutsideDomain = config.getBoolean(
                "parser.ignore.outlinks.outside.domain", false);

        // instanciate Tika
        long start = System.currentTimeMillis();
        tika = new Tika();
        long end = System.currentTimeMillis();

        LOG.debug("Tika loaded in " + (end - start) + " msec");

        this.collector = collector;

        // Register a "MultiCountMetric" to count different events in this bolt
        // Storm will emit the counts every n seconds to a special bolt via a
        // system stream
        // The data can be accessed by registering a "MetricConsumer" in the
        // topology
        this.eventCounter = context.registerMetric("parser-counter",
                new MultiCountMetric(), 5);

        // Register a MeanMulti metric to keep track of means of various metrics
        // in this bolt
        // Ideally this can be a histogram, but that requires a custom
        // MetricReducer
        this.eventStats = context.registerMetric("parser-stats",
                new MultiReducedMetric(new MeanReducer()), 5);
    }

    public void execute(Tuple tuple) {
        byte[] content = tuple.getBinaryByField("content");
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
            eventCounter.scope(
                    "error_content_parsing_" + e.getClass().getSimpleName())
                    .incr();
            collector.fail(tuple);
            return;
        } finally {
            try {
                bais.close();
            } catch (IOException e) {
                LOG.error("Exception while closing stream", e);
            }
        }

        long end = System.currentTimeMillis();
        long duration = end - start;

        LOG.info("Parsed " + url + " in " + duration + " msec");
        eventStats.scope("content_parsing_ms").update(duration);

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
            collector.fail(tuple);
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
                eventCounter.scope(
                        "error_out_link_parsing_"
                                + e.getClass().getSimpleName()).incr();
                continue;
            }

            // filter the urls
            if (filters != null) {
                urlOL = filters.filter(urlOL);
                if (urlOL == null) {
                    eventCounter.scope("outlink_filtered").incr();
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
                    eventCounter.scope("outlink_outsideHost").incr();
                    continue;
                }
            }

            if (urlOL != null) {
                slinks.add(urlOL);
                eventCounter.scope("outlink_kept").incr();
            }
        }

        // add parse md to metadata
        for (String k : md.names()) {
            // TODO handle mutliple values
            String[] values = md.getValues(k);
            metadata.put("parse." + k, values);
        }

        // generate output
        List<Object> fields = new ArrayList<Object>(4);
        fields.add(url);
        fields.add(content);
        fields.add(metadata);
        fields.add(text.trim());
        fields.add(slinks);

        collector.emit(fields);
        collector.ack(tuple);

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // output of this module is the list of fields to index
        // with at least the URL, text content

        declarer.declare(new Fields("url", "content", "metadata", "text",
                "outlinks"));
    }

}