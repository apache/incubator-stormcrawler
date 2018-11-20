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

package com.digitalpebble.stormcrawler.bolt;

import static com.digitalpebble.stormcrawler.Constants.StatusStreamName;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.metric.api.MeanReducer;
import org.apache.storm.metric.api.ReducedMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.Constants;
import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.parse.Outlink;
import com.digitalpebble.stormcrawler.parse.ParseFilter;
import com.digitalpebble.stormcrawler.parse.ParseFilters;
import com.digitalpebble.stormcrawler.parse.ParseResult;
import com.digitalpebble.stormcrawler.persistence.DefaultScheduler;
import com.digitalpebble.stormcrawler.persistence.Status;
import com.digitalpebble.stormcrawler.protocol.HttpHeaders;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import com.google.common.primitives.Bytes;

import crawlercommons.sitemaps.AbstractSiteMap;
import crawlercommons.sitemaps.Namespace;
import crawlercommons.sitemaps.SiteMap;
import crawlercommons.sitemaps.SiteMapIndex;
import crawlercommons.sitemaps.SiteMapParser;
import crawlercommons.sitemaps.SiteMapURL;
import crawlercommons.sitemaps.SiteMapURL.ChangeFrequency;
import crawlercommons.sitemaps.UnknownFormatException;

/**
 * Extracts URLs from sitemap files. The parsing is triggered by the presence of
 * 'isSitemap=true' in the metadata. Any tuple which does not have this
 * key/value in the metadata is simply passed on to the default stream, whereas
 * any URLs extracted from the sitemaps is sent to the 'status' field.
 */
@SuppressWarnings("serial")
public class SiteMapParserBolt extends StatusEmitterBolt {

    public static final String isSitemapKey = "isSitemap";

    private static final org.slf4j.Logger LOG = LoggerFactory
            .getLogger(SiteMapParserBolt.class);

    private static final byte[] clue = Namespace.SITEMAP.getBytes();

    private SiteMapParser parser;

    private boolean sniffWhenNoSMKey = false;

    private ParseFilter parseFilters;
    private int filterHoursSinceModified = -1;

    private int maxOffsetGuess = 300;

    private ReducedMetric averagedMetrics;

    /** Delay in minutes used for scheduling sub-sitemaps **/
    private int scheduleSitemapsWithDelay = -1;

    @Override
    public void execute(Tuple tuple) {
        Metadata metadata = (Metadata) tuple.getValueByField("metadata");
        byte[] content = tuple.getBinaryByField("content");
        String url = tuple.getStringByField("url");

        String ct = metadata.getFirstValue(HttpHeaders.CONTENT_TYPE);

        boolean looksLikeSitemap = sniff(content);
        // can force the mimetype as we know it is XML
        if (looksLikeSitemap) {
            LOG.info("{} detected as sitemap based on content", url);
            ct = "application/xml";
        }

        String isSitemap = metadata.getFirstValue(isSitemapKey);

        boolean treatAsSM = false;

        if ("true".equalsIgnoreCase(isSitemap)) {
            treatAsSM = true;
        }

        // doesn't have the key and want to rely on the clue
        if (isSitemap == null && sniffWhenNoSMKey && looksLikeSitemap) {
            treatAsSM = true;
        }

        // decided that it is not a sitemap file
        if (!treatAsSM) {
            // just pass it on
            this.collector.emit(tuple, tuple.getValues());
            this.collector.ack(tuple);
            return;
        }

        List<Outlink> outlinks;
        try {
            outlinks = parseSiteMap(url, content, ct, metadata);
        } catch (Exception e) {
            // exception while parsing the sitemap
            String errorMessage = "Exception while parsing " + url + ": " + e;
            LOG.error(errorMessage);
            // send to status stream in case another component wants to update
            // its status
            metadata.setValue(Constants.STATUS_ERROR_SOURCE, "sitemap parsing");
            metadata.setValue(Constants.STATUS_ERROR_MESSAGE, errorMessage);
            collector.emit(Constants.StatusStreamName, tuple, new Values(url,
                    metadata, Status.ERROR));
            collector.ack(tuple);
            return;
        }

        // mark the current doc as a sitemap
        // as it won't have the k/v if it is a redirected sitemap
        metadata.setValue(isSitemapKey, "true");

        // apply the parse filters if any to the current document
        ParseResult parse = new ParseResult(outlinks);
        parse.set(url, metadata);

        // apply the parse filters if any
        try {
            parseFilters.filter(url, content, null, parse);
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
            return;
        }

        // send to status stream
        for (Outlink ol : parse.getOutlinks()) {
            Values v = new Values(ol.getTargetURL(), ol.getMetadata(),
                    Status.DISCOVERED);
            collector.emit(Constants.StatusStreamName, tuple, v);
        }

        // marking the main URL as successfully fetched
        // regardless of whether we got a parse exception or not
        collector.emit(Constants.StatusStreamName, tuple, new Values(url,
                metadata, Status.FETCHED));
        collector.ack(tuple);
    }

    private List<Outlink> parseSiteMap(String url, byte[] content,
            String contentType, Metadata parentMetadata)
            throws UnknownFormatException, IOException {

        URL sURL = new URL(url);
        long start = System.currentTimeMillis();
        AbstractSiteMap siteMap;
        // let the parser guess what the mimetype is
        if (StringUtils.isBlank(contentType)
                || contentType.contains("octet-stream")) {
            siteMap = parser.parseSiteMap(content, sURL);
        } else {
            siteMap = parser.parseSiteMap(contentType, content, sURL);
        }
        long end = System.currentTimeMillis();
        averagedMetrics.update(end - start);

        List<Outlink> links = new ArrayList<>();

        if (siteMap.isIndex()) {
            SiteMapIndex smi = (SiteMapIndex) siteMap;
            Collection<AbstractSiteMap> subsitemaps = smi.getSitemaps();

            Calendar rightNow = Calendar.getInstance();
            rightNow.add(Calendar.HOUR, -filterHoursSinceModified);

            int delay = 0;

            // keep the subsitemaps as outlinks
            // they will be fetched and parsed in the following steps
            Iterator<AbstractSiteMap> iter = subsitemaps.iterator();
            while (iter.hasNext()) {
                AbstractSiteMap asm = iter.next();
                String target = asm.getUrl().toExternalForm();

                Date lastModified = asm.getLastModified();
                String lastModifiedValue = "";
                if (lastModified != null) {
                    // filter based on the published date
                    if (filterHoursSinceModified != -1) {
                        if (lastModified.before(rightNow.getTime())) {
                            LOG.info(
                                    "{} has a modified date {} which is more than {} hours old",
                                    target, lastModified.toString(),
                                    filterHoursSinceModified);
                            continue;
                        }
                    }
                    lastModifiedValue = lastModified.toString();
                }

                Outlink ol = filterOutlink(sURL, target, parentMetadata,
                        isSitemapKey, "true", "sitemap.lastModified",
                        lastModifiedValue);
                if (ol == null) {
                    continue;
                }

                // add a delay
                if (this.scheduleSitemapsWithDelay > 0) {
                    if (delay > 0) {
                        ol.getMetadata().setValue(
                                DefaultScheduler.DELAY_METADATA,
                                Integer.toString(delay));
                    }
                    delay += this.scheduleSitemapsWithDelay;
                }

                links.add(ol);
                LOG.debug("{} : [sitemap] {}", url, target);
            }
        }
        // sitemap files
        else {
            SiteMap sm = (SiteMap) siteMap;
            // TODO see what we can do with the LastModified info
            Collection<SiteMapURL> sitemapURLs = sm.getSiteMapUrls();
            Iterator<SiteMapURL> iter = sitemapURLs.iterator();
            while (iter.hasNext()) {
                SiteMapURL smurl = iter.next();
                // TODO handle priority in metadata
                double priority = smurl.getPriority();
                // TODO convert the frequency into a numerical value and handle
                // it in metadata
                ChangeFrequency freq = smurl.getChangeFrequency();

                String target = smurl.getUrl().toExternalForm();
                String lastModifiedValue = "";
                Date lastModified = smurl.getLastModified();
                if (lastModified != null) {
                    // filter based on the published date
                    if (filterHoursSinceModified != -1) {
                        Calendar rightNow = Calendar.getInstance();
                        rightNow.add(Calendar.HOUR, -filterHoursSinceModified);
                        if (lastModified.before(rightNow.getTime())) {
                            LOG.info(
                                    "{} has a modified date {} which is more than {} hours old",
                                    target, lastModified.toString(),
                                    filterHoursSinceModified);
                            continue;
                        }
                    }
                    lastModifiedValue = lastModified.toString();
                }

                Outlink ol = filterOutlink(sURL, target, parentMetadata,
                        isSitemapKey, "false", "sitemap.lastModified",
                        lastModifiedValue);
                if (ol == null) {
                    continue;
                }
                links.add(ol);
                LOG.debug("{} : [sitemap] {}", url, target);
            }
        }

        return links;
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        parser = new SiteMapParser(false);
        sniffWhenNoSMKey = ConfUtils.getBoolean(stormConf,
                "sitemap.sniffContent", false);
        filterHoursSinceModified = ConfUtils.getInt(stormConf,
                "sitemap.filter.hours.since.modified", -1);
        parseFilters = ParseFilters.fromConf(stormConf);
        maxOffsetGuess = ConfUtils.getInt(stormConf, "sitemap.offset.guess",
                300);
        averagedMetrics = context.registerMetric(
                "sitemap_average_processing_time", new ReducedMetric(
                        new MeanReducer()), 30);
        scheduleSitemapsWithDelay = ConfUtils.getInt(stormConf,
                "sitemap.schedule.delay", scheduleSitemapsWithDelay);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        super.declareOutputFields(declarer);
        declarer.declare(new Fields("url", "content", "metadata"));
    }

    /**
     * Examines the first bytes of the content for a clue of whether this
     * document is a sitemap, based on namespaces. Works for XML and
     * non-compressed documents only.
     **/
    private final boolean sniff(byte[] content) {
        byte[] beginning = content;
        if (content.length > maxOffsetGuess && maxOffsetGuess > 0) {
            beginning = Arrays.copyOfRange(content, 0, maxOffsetGuess);
        }
        int position = Bytes.indexOf(beginning, clue);
        if (position != -1) {
            return true;
        }
        return false;
    }

}
