/**
 * Licensed to DigitalPebble Ltd under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * DigitalPebble licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.digitalpebble.storm.crawler.bolt;

import static com.digitalpebble.storm.crawler.Constants.StatusStreamName;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.http.entity.ContentType;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;

import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.digitalpebble.storm.crawler.Constants;
import com.digitalpebble.storm.crawler.Metadata;
import com.digitalpebble.storm.crawler.filtering.URLFilters;
import com.digitalpebble.storm.crawler.parse.JSoupDOMBuilder;
import com.digitalpebble.storm.crawler.parse.Outlink;
import com.digitalpebble.storm.crawler.parse.ParseData;
import com.digitalpebble.storm.crawler.parse.ParseFilter;
import com.digitalpebble.storm.crawler.parse.ParseFilters;
import com.digitalpebble.storm.crawler.parse.ParseResult;
import com.digitalpebble.storm.crawler.persistence.Status;
import com.digitalpebble.storm.crawler.protocol.HttpHeaders;
import com.digitalpebble.storm.crawler.util.ConfUtils;
import com.digitalpebble.storm.crawler.util.MetadataTransfer;
import com.digitalpebble.storm.crawler.util.RobotsTags;
import com.ibm.icu.text.CharsetDetector;
import com.ibm.icu.text.CharsetMatch;

/**
 * Parser for HTML documents only which uses ICU4J to detect the charset
 * encoding. Kindly donated to storm-crawler by shopstyle.com.
 */
@SuppressWarnings("serial")
public class JSoupParserBolt extends BaseRichBolt {

    /** Metadata key name for tracking the anchors */
    public static final String ANCHORS_KEY_NAME = "anchors";

    private static final Logger LOG = LoggerFactory
            .getLogger(JSoupParserBolt.class);

    private OutputCollector collector;

    private MultiCountMetric eventCounter;

    private ParseFilter parseFilters = null;

    private URLFilters urlFilters = null;

    private MetadataTransfer metadataTransfer;

    private boolean trackAnchors = true;

    private boolean emitOutlinks = true;

    private boolean robots_noFollow_strict = true;

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void prepare(Map conf, TopologyContext context,
            OutputCollector collector) {
        this.collector = collector;

        eventCounter = context.registerMetric(this.getClass().getSimpleName(),
                new MultiCountMetric(), 10);

        parseFilters = ParseFilters.emptyParseFilter;

        String parseconfigfile = ConfUtils.getString(conf,
                "parsefilters.config.file", "parsefilters.json");
        if (parseconfigfile != null) {
            try {
                parseFilters = new ParseFilters(conf, parseconfigfile);
            } catch (IOException e) {
                LOG.error("Exception caught while loading the ParseFilters");
                throw new RuntimeException(
                        "Exception caught while loading the ParseFilters", e);
            }
        }

        urlFilters = URLFilters.emptyURLFilters;
        emitOutlinks = ConfUtils.getBoolean(conf, "parser.emitOutlinks", true);

        if (emitOutlinks) {
            String urlconfigfile = ConfUtils.getString(conf,
                    "urlfilters.config.file", "urlfilters.json");

            if (urlconfigfile != null) {
                try {
                    urlFilters = new URLFilters(conf, urlconfigfile);
                } catch (IOException e) {
                    LOG.error("Exception caught while loading the URLFilters");
                    throw new RuntimeException(
                            "Exception caught while loading the URLFilters", e);
                }
            }
        }

        trackAnchors = ConfUtils.getBoolean(conf, "track.anchors", true);

        robots_noFollow_strict = ConfUtils.getBoolean(conf,
                RobotsTags.ROBOTS_NO_FOLLOW_STRICT, true);

        metadataTransfer = MetadataTransfer.getInstance(conf);
    }

    @Override
    public void execute(Tuple tuple) {

        byte[] content = tuple.getBinaryByField("content");
        String url = tuple.getStringByField("url");
        Metadata metadata = (Metadata) tuple.getValueByField("metadata");

        // check that its content type is HTML
        // look at value found in HTTP headers
        boolean CT_OK = false;
        String httpCT = metadata.getFirstValue(HttpHeaders.CONTENT_TYPE);
        if (StringUtils.isNotBlank(httpCT)) {
            if (httpCT.toLowerCase().contains("html")) {
                CT_OK = true;
            }
        }
        // simply ignore cases where the content type has not been set
        // TODO sniff content with Tika?
        else {
            CT_OK = true;
        }

        if (!CT_OK) {
            String errorMessage = "Exception content-type " + httpCT + " for "
                    + url;
            RuntimeException e = new RuntimeException(errorMessage);
            handleException(url, e, metadata, tuple, "content-type checking",
                    errorMessage);
            return;
        }

        LOG.info("Parsing : starting {}", url);

        long start = System.currentTimeMillis();

        String charset = getContentCharset(content, metadata);

        // get the robots tags from the fetch metadata
        RobotsTags robotsTags = new RobotsTags(metadata);

        Map<String, List<String>> slinks;
        String text;
        DocumentFragment fragment;
        try (ByteArrayInputStream bais = new ByteArrayInputStream(content)) {
            org.jsoup.nodes.Document jsoupDoc = Jsoup.parse(bais, charset, url);

            fragment = JSoupDOMBuilder.jsoup2HTML(jsoupDoc);

            // extracts the robots directives from the meta tags
            robotsTags.extractMetaTags(fragment);

            // store a normalised representation in metadata
            // so that the indexer is aware of it
            robotsTags.normaliseToMetadata(metadata);

            // do not extract the links if no follow has been set
            // and we are in strict mode
            if (robotsTags.isNoFollow() && robots_noFollow_strict) {
                slinks = new HashMap<String, List<String>>(0);
            } else {
                Elements links = jsoupDoc.select("a[href]");
                slinks = new HashMap<String, List<String>>(links.size());
                for (Element link : links) {
                    // abs:href tells jsoup to return fully qualified domains
                    // for
                    // relative urls.
                    // e.g.: /foo will resolve to http://shopstyle.com/foo
                    String targetURL = link.attr("abs:href");

                    // nofollow
                    boolean noFollow = "nofollow".equalsIgnoreCase(link
                            .attr("rel"));
                    // remove altogether
                    if (noFollow && robots_noFollow_strict) {
                        continue;
                    }

                    // link not specifically marked as no follow
                    // but whole page is
                    if (!noFollow && robotsTags.isNoFollow()) {
                        noFollow = true;
                    }

                    String anchor = link.text();
                    if (StringUtils.isNotBlank(targetURL)) {
                        // any existing anchors for the same target?
                        List<String> anchors = slinks.get(targetURL);
                        if (anchors == null) {
                            anchors = new LinkedList<String>();
                            slinks.put(targetURL, anchors);
                        }
                        // track the anchors only if no follow is false
                        if (!noFollow && StringUtils.isNotBlank(anchor)) {
                            anchors.add(anchor);
                        }
                    }
                }
            }

            text = jsoupDoc.body().text();

        } catch (Throwable e) {
            String errorMessage = "Exception while parsing " + url + ": " + e;
            handleException(url, e, metadata, tuple, "content parsing",
                    errorMessage);
            return;
        }

        // store identified charset in md
        metadata.setValue("parse.Content-Encoding", charset);

        long duration = System.currentTimeMillis() - start;

        LOG.info("Parsed {} in {} msec", url, duration);

        List<Outlink> outlinks = toOutlinks(url, metadata, slinks);

        ParseResult parse = new ParseResult();
        parse.setOutlinks(outlinks);

        // parse data of the parent URL
        ParseData parseData = parse.get(url);
        parseData.setMetadata(metadata);
        parseData.setText(text);
        parseData.setContent(content);

        // apply the parse filters if any
        try {
            parseFilters.filter(url, content, fragment, parse);
        } catch (RuntimeException e) {

            String errorMessage = "Exception while running parse filters on "
                    + url + ": " + e;
            handleException(url, e, metadata, tuple, "content filtering",
                    errorMessage);
            return;
        }

        if (emitOutlinks) {
            for (Outlink outlink : outlinks) {
                collector.emit(
                        StatusStreamName,
                        tuple,
                        new Values(outlink.getTargetURL(), outlink
                                .getMetadata(), Status.DISCOVERED));
            }
        }

        // emit each document/subdocument in the ParseResult object
        // there should be at least one ParseData item for the "parent" URL

        for (Map.Entry<String, ParseData> doc : parse) {
            ParseData parseDoc = doc.getValue();

            collector.emit(new Values(doc.getKey(), parseDoc.getContent(),
                    parseDoc.getMetadata(), parseDoc.getText()));
        }

        collector.ack(tuple);
        eventCounter.scope("tuple_success").incr();
    }

    private void handleException(String url, Throwable e, Metadata metadata,
            Tuple tuple, String errorSource, String errorMessage) {
        LOG.error(errorMessage);
        // send to status stream in case another component wants to update
        // its status
        metadata.setValue(Constants.STATUS_ERROR_SOURCE, errorSource);
        metadata.setValue(Constants.STATUS_ERROR_MESSAGE, errorMessage);
        collector.emit(StatusStreamName, tuple, new Values(url, metadata,
                Status.ERROR));
        collector.ack(tuple);
        // Increment metric that is context specific
        String s = "error_" + errorSource.replaceAll(" ", "_") + "_";
        eventCounter.scope(s + e.getClass().getSimpleName()).incrBy(1);
        // Increment general metric
        eventCounter.scope("parse exception").incrBy(1);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // output of this module is the list of fields to index
        // with at least the URL, text content
        declarer.declare(new Fields("url", "content", "metadata", "text"));
        declarer.declareStream(StatusStreamName, new Fields("url", "metadata",
                "status"));
    }

    private String getContentCharset(byte[] content, Metadata metadata) {
        String charset = null;

        // check if the server specified a charset
        String specifiedContentType = metadata
                .getFirstValue(HttpHeaders.CONTENT_TYPE);
        try {
            if (specifiedContentType != null) {
                ContentType parsedContentType = ContentType
                        .parse(specifiedContentType);
                charset = parsedContentType.getCharset().name();
            }
        } catch (Exception e) {
            charset = null;
        }

        // filter HTML tags
        CharsetDetector detector = new CharsetDetector();
        detector.enableInputFilter(true);
        // give it a hint
        detector.setDeclaredEncoding(charset);
        detector.setText(content);
        try {
            CharsetMatch charsetMatch = detector.detect();
            if (charsetMatch != null) {
                charset = charsetMatch.getName();
            }
        } catch (Exception e) {
            // ignore and leave the charset as-is
        }
        return charset;
    }

    private List<Outlink> toOutlinks(String url, Metadata metadata,
            Map<String, List<String>> slinks) {
        List<Outlink> outlinks = new LinkedList<Outlink>();
        URL sourceUrl;
        try {
            sourceUrl = new URL(url);
        } catch (MalformedURLException e) {
            // we would have known by now as previous components check whether
            // the URL is valid
            LOG.error("MalformedURLException on {}", url);
            eventCounter.scope("error_invalid_source_url").incrBy(1);
            return outlinks;
        }

        Map<String, List<String>> linksKept = new HashMap<String, List<String>>();

        for (Map.Entry<String, List<String>> linkEntry : slinks.entrySet()) {
            String targetURL = linkEntry.getKey();
            // filter the urls
            if (urlFilters != null) {
                targetURL = urlFilters.filter(sourceUrl, metadata, targetURL);
                if (targetURL == null) {
                    eventCounter.scope("outlink_filtered").incr();
                    continue;
                }
            }
            // the link has survived the various filters
            if (targetURL != null) {
                List<String> anchors = linkEntry.getValue();
                linksKept.put(targetURL, anchors);
                eventCounter.scope("outlink_kept").incr();
            }
        }

        for (String outlink : linksKept.keySet()) {
            // configure which metadata gets inherited from parent
            Metadata linkMetadata = metadataTransfer.getMetaForOutlink(outlink,
                    url, metadata);
            Outlink ol = new Outlink(outlink);
            // add the anchors to the metadata?
            if (trackAnchors) {
                List<String> anchors = linksKept.get(outlink);
                if (anchors.size() > 0) {
                    linkMetadata.addValues(ANCHORS_KEY_NAME, anchors);
                    // sets the first anchor
                    ol.setAnchor(anchors.get(0));
                }
            }
            ol.setMetadata(linkMetadata);
            outlinks.add(ol);
        }
        return outlinks;
    }
}
