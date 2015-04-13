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

package com.digitalpebble.storm.crawler.tika;

import static com.digitalpebble.storm.crawler.Constants.StatusStreamName;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.html.dom.HTMLDocumentImpl;
import org.apache.tika.Tika;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.html.HtmlMapper;
import org.apache.tika.parser.html.IdentityHtmlMapper;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.Link;
import org.apache.tika.sax.LinkContentHandler;
import org.apache.tika.sax.TeeContentHandler;
import org.apache.tika.sax.XHTMLContentHandler;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;
import org.xml.sax.ContentHandler;

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
import com.digitalpebble.storm.crawler.parse.Outlink;
import com.digitalpebble.storm.crawler.parse.ParseFilter;
import com.digitalpebble.storm.crawler.parse.ParseFilters;
import com.digitalpebble.storm.crawler.persistence.Status;
import com.digitalpebble.storm.crawler.util.ConfUtils;
import com.digitalpebble.storm.crawler.util.MetadataTransfer;
import com.digitalpebble.storm.crawler.util.URLUtil;

/**
 * Uses Tika to parse the output of a fetch and extract text + metadata
 */
@SuppressWarnings("serial")
public class ParserBolt extends BaseRichBolt {

    private Tika tika;

    private URLFilters urlFilters = null;
    private ParseFilter parseFilters = null;

    private OutputCollector collector;

    private static final org.slf4j.Logger LOG = LoggerFactory
            .getLogger(ParserBolt.class);

    private MultiCountMetric eventCounter;

    private boolean upperCaseElementNames = true;
    private Class<?> HTMLMapperClass = IdentityHtmlMapper.class;

    private MetadataTransfer metadataTransfer;
    private boolean emitOutlinks = true;

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void prepare(Map conf, TopologyContext context,
            OutputCollector collector) {

        emitOutlinks = ConfUtils.getBoolean(conf, "parser.emitOutlinks", true);

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
        } else {
            urlFilters = URLFilters.emptyURLFilters;
        }

        String parseconfigfile = ConfUtils.getString(conf,
                "parsefilters.config.file", "parsefilters.json");

        parseFilters = ParseFilters.emptyParseFilter;

        if (parseconfigfile != null) {
            try {
                parseFilters = new ParseFilters(conf, parseconfigfile);
            } catch (IOException e) {
                LOG.error("Exception caught while loading the ParseFilters");
                throw new RuntimeException(
                        "Exception caught while loading the ParseFilters", e);
            }
        }

        upperCaseElementNames = ConfUtils.getBoolean(conf,
                "parser.uppercase.element.names", true);

        String htmlmapperClassName = ConfUtils.getString(conf,
                "parser.htmlmapper.classname",
                "org.apache.tika.parser.html.IdentityHtmlMapper");

        try {
            HTMLMapperClass = Class.forName(htmlmapperClassName);
            boolean interfaceOK = HtmlMapper.class
                    .isAssignableFrom(HTMLMapperClass);
            if (!interfaceOK) {
                throw new RuntimeException("Class " + htmlmapperClassName
                        + " does not implement HtmlMapper");
            }
        } catch (ClassNotFoundException e) {
            LOG.error("Can't load class {}", htmlmapperClassName);
            throw new RuntimeException("Can't load class "
                    + htmlmapperClassName);
        }

        // instanciate Tika
        long start = System.currentTimeMillis();
        tika = new Tika();
        long end = System.currentTimeMillis();

        LOG.debug("Tika loaded in {} msec", (end - start));

        this.collector = collector;

        this.eventCounter = context.registerMetric(this.getClass()
                .getSimpleName(), new MultiCountMetric(), 10);

        this.metadataTransfer = MetadataTransfer.getInstance(conf);
    }

    @Override
    public void execute(Tuple tuple) {
        eventCounter.scope("tuple_in").incrBy(1);

        byte[] content = tuple.getBinaryByField("content");

        String url = tuple.getStringByField("url");
        Metadata metadata = (Metadata) tuple.getValueByField("metadata");

        long start = System.currentTimeMillis();

        // rely on mime-type provided by server or guess?

        ByteArrayInputStream bais = new ByteArrayInputStream(content);
        org.apache.tika.metadata.Metadata md = new org.apache.tika.metadata.Metadata();

        LinkContentHandler linkHandler = new LinkContentHandler();
        ContentHandler textHandler = new BodyContentHandler(-1);
        TeeContentHandler teeHandler = new TeeContentHandler(linkHandler,
                textHandler);
        ParseContext parseContext = new ParseContext();

        try {
            parseContext.set(HtmlMapper.class,
                    (HtmlMapper) HTMLMapperClass.newInstance());
        } catch (Exception e) {
            LOG.error("Exception while specifying HTMLMapper {}", url, e);
        }

        // build a DOM if required by the parseFilters
        DocumentFragment root = null;
        if (parseFilters.needsDOM()) {
            HTMLDocumentImpl doc = new HTMLDocumentImpl();
            doc.setErrorChecking(false);
            root = doc.createDocumentFragment();
            DOMBuilder domhandler = new DOMBuilder(doc, root);
            domhandler.setUpperCaseElementNames(upperCaseElementNames);
            domhandler.setDefaultNamespaceURI(XHTMLContentHandler.XHTML);
            teeHandler = new TeeContentHandler(linkHandler, textHandler,
                    domhandler);
        }

        // parse
        String text;
        try {
            tika.getParser().parse(bais, teeHandler, md, parseContext);
            text = textHandler.toString();
        } catch (Exception e) {
            String errorMessage = "Exception while parsing " + url + ": " + e;
            LOG.error(errorMessage);
            // send to status stream in case another component wants to update
            // its status
            metadata.setValue(Constants.STATUS_ERROR_SOURCE, "content parsing");
            metadata.setValue(Constants.STATUS_ERROR_MESSAGE, errorMessage);
            collector.emit(StatusStreamName, tuple, new Values(url, metadata,
                    Status.ERROR));
            collector.ack(tuple);
            // Increment metric that is context specific
            eventCounter.scope(
                    "error_content_parsing_" + e.getClass().getSimpleName())
                    .incrBy(1);
            // Increment general metric
            eventCounter.scope("parse exception").incrBy(1);
            return;
        } finally {
            try {
                bais.close();
            } catch (IOException e) {
                LOG.error("Exception while closing stream", e);
            }
        }

        // add parse md to metadata
        for (String k : md.names()) {
            String[] values = md.getValues(k);
            metadata.setValues("parse." + k, values);
        }

        long duration = System.currentTimeMillis() - start;

        LOG.info("Parsed {} in {} msec", url, duration);

        // filter and convert the outlinks
        List<Outlink> outlinks = toOutlinks(url, linkHandler.getLinks(),
                metadata);

        // apply the parse filters if any
        try {
            parseFilters.filter(url, content, root, metadata, outlinks);
        } catch (RuntimeException e) {
            String errorMessage = "Exception while running parse filters on "
                    + url + ": " + e;
            LOG.error(errorMessage);
            metadata.setValue(Constants.STATUS_ERROR_SOURCE,
                    "content filtering");
            metadata.setValue(Constants.STATUS_ERROR_MESSAGE, errorMessage);
            collector.emit(StatusStreamName, tuple, new Values(url, metadata,
                    Status.ERROR));
            collector.ack(tuple);
            // Increment metric that is context specific
            eventCounter.scope(
                    "error_content_filtering_" + e.getClass().getSimpleName())
                    .incrBy(1);
            // Increment general metric
            eventCounter.scope("parse exception").incrBy(1);
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

        collector.emit(tuple, new Values(url, content, metadata, text.trim()));
        collector.ack(tuple);
        eventCounter.scope("tuple_success").incrBy(1);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url", "content", "metadata", "text"));
        declarer.declareStream(StatusStreamName, new Fields("url", "metadata",
                "status"));
    }

    private List<Outlink> toOutlinks(String parentURL, List<Link> links,
            Metadata parentMetadata) {

        List<Outlink> outlinks = new ArrayList<Outlink>(links.size());

        URL url_;
        try {
            url_ = new URL(parentURL);
        } catch (MalformedURLException e1) {
            // we would have known by now as previous
            // components check whether the URL is valid
            LOG.error("MalformedURLException on {}", parentURL);
            eventCounter.scope("error_invalid_source_url").incrBy(1);
            return outlinks;
        }

        for (Link l : links) {
            if (StringUtils.isBlank(l.getUri())) {
                continue;
            }
            String urlOL = null;

            // build an absolute URL
            try {
                URL tmpURL = URLUtil.resolveURL(url_, l.getUri());
                urlOL = tmpURL.toExternalForm();
            } catch (MalformedURLException e) {
                LOG.debug("MalformedURLException on {}", l.getUri());
                eventCounter
                        .scope("error_outlink_parsing_"
                                + e.getClass().getSimpleName()).incrBy(1);
                continue;
            }

            // applies the URL filters
            if (urlFilters != null) {
                urlOL = urlFilters.filter(url_, parentMetadata, urlOL);
                if (urlOL == null) {
                    eventCounter.scope("outlink_filtered").incrBy(1);
                    continue;
                }
            }

            eventCounter.scope("outlink_kept").incrBy(1);

            Outlink ol = new Outlink(urlOL);
            // add the anchor
            ol.setAnchor(l.getText());

            // get the metadata for the outlink from the parent ones
            ol.setMetadata(metadataTransfer.getMetaForOutlink(urlOL, parentURL,
                    parentMetadata));

            outlinks.add(ol);
        }
        return outlinks;
    }

}