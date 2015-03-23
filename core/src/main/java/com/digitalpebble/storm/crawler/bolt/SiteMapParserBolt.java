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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.LoggerFactory;

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
import com.digitalpebble.storm.crawler.persistence.Status;
import com.digitalpebble.storm.crawler.protocol.HttpHeaders;
import com.digitalpebble.storm.crawler.util.ConfUtils;
import com.digitalpebble.storm.crawler.util.MetadataTransfer;
import com.digitalpebble.storm.crawler.util.URLUtil;

import crawlercommons.sitemaps.AbstractSiteMap;
import crawlercommons.sitemaps.SiteMap;
import crawlercommons.sitemaps.SiteMapIndex;
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
public class SiteMapParserBolt extends BaseRichBolt {
    public static final String isSitemapKey = "isSitemap";

    private static final org.slf4j.Logger LOG = LoggerFactory
            .getLogger(SiteMapParserBolt.class);

    private OutputCollector collector;
    private boolean strictMode = false;
    private MetadataTransfer metadataTransfer;
    private URLFilters urlFilters;

    @Override
    public void execute(Tuple tuple) {
        Metadata metadata = (Metadata) tuple.getValueByField("metadata");

        // TODO check that we have the right number of fields ?
        String isSitemap = metadata.getFirstValue(isSitemapKey);
        if (!Boolean.valueOf(isSitemap)) {
            // just pass it on
            this.collector.emit(tuple.getValues());
            this.collector.ack(tuple);
            return;
        }

        // it does have the right key/value
        byte[] content = tuple.getBinaryByField("content");
        String url = tuple.getStringByField("url");
        String ct = metadata.getFirstValue(HttpHeaders.CONTENT_TYPE);

        List<Values> outlinks = Collections.emptyList();
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
            collector.emit(Constants.StatusStreamName, new Values(url,
                    metadata, Status.ERROR));
            this.collector.ack(tuple);
            return;
        }

        // send to status stream
        for (Values ol : outlinks) {
            collector.emit(Constants.StatusStreamName, ol);
        }

        // marking the main URL as successfully fetched
        // regardless of whether we got a parse exception or not
        collector.emit(Constants.StatusStreamName, new Values(url, metadata,
                Status.FETCHED));
        this.collector.ack(tuple);
    }

    private List<Values> parseSiteMap(String url, byte[] content,
            String contentType, Metadata parentMetadata)
            throws UnknownFormatException, IOException {

        crawlercommons.sitemaps.SiteMapParser parser = new crawlercommons.sitemaps.SiteMapParser(
                strictMode);

        URL sURL = new URL(url);
        AbstractSiteMap siteMap = parser.parseSiteMap(contentType, content,
                sURL);

        List<Values> links = new ArrayList<Values>();

        if (siteMap.isIndex()) {
            SiteMapIndex smi = ((SiteMapIndex) siteMap);
            Collection<AbstractSiteMap> subsitemaps = smi.getSitemaps();
            // keep the subsitemaps as outlinks
            // they will be fetched and parsed in the following steps
            Iterator<AbstractSiteMap> iter = subsitemaps.iterator();
            while (iter.hasNext()) {
                String target = iter.next().getUrl().toExternalForm();

                // build an absolute URL
                try {
                    target = URLUtil.resolveURL(sURL, target).toExternalForm();
                } catch (MalformedURLException e) {
                    LOG.debug("MalformedURLException on {}", target);
                    continue;
                }

                // apply filtering to outlinks
                if (urlFilters != null) {
                    target = urlFilters.filter(sURL, parentMetadata, target);
                }

                if (StringUtils.isBlank(target))
                    continue;

                // configure which metadata gets inherited from parent
                Metadata metadata = metadataTransfer.getMetaForOutlink(target,
                        url, parentMetadata);
                metadata.setValue(isSitemapKey, "true");

                Values ol = new Values(target, metadata, Status.DISCOVERED);
                links.add(ol);
                LOG.debug("{} : [sitemap] {}", url, target);
            }
        }
        // sitemap files
        else {
            SiteMap sm = ((SiteMap) siteMap);
            // TODO see what we can do with the LastModified info
            Collection<SiteMapURL> sitemapURLs = sm.getSiteMapUrls();
            Iterator<SiteMapURL> iter = sitemapURLs.iterator();
            while (iter.hasNext()) {
                SiteMapURL smurl = iter.next();
                double priority = smurl.getPriority();
                // TODO handle priority in metadata
                ChangeFrequency freq = smurl.getChangeFrequency();
                // TODO convert the frequency into a numerical value and handle
                // it in metadata

                String target = smurl.getUrl().toExternalForm();

                // build an absolute URL
                try {
                    target = URLUtil.resolveURL(sURL, target).toExternalForm();
                } catch (MalformedURLException e) {
                    LOG.debug("MalformedURLException on {}", target);
                    continue;
                }

                // apply filtering to outlinks
                if (urlFilters != null) {
                    target = urlFilters.filter(sURL, parentMetadata, target);
                }

                if (StringUtils.isBlank(target))
                    continue;

                // configure which metadata gets inherited from parent
                Metadata metadata = metadataTransfer.getMetaForOutlink(target,
                        url, parentMetadata);
                metadata.setValue(isSitemapKey, "false");

                Values ol = new Values(target, metadata, Status.DISCOVERED);
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
        this.collector = collector;
        this.metadataTransfer = MetadataTransfer.getInstance(stormConf);

        String urlconfigfile = ConfUtils.getString(stormConf,
                "urlfilters.config.file", "urlfilters.json");
        if (urlconfigfile != null)
            try {
                urlFilters = new URLFilters(stormConf, urlconfigfile);
            } catch (IOException e) {
                LOG.error("Exception caught while loading the URLFilters");
                throw new RuntimeException(
                        "Exception caught while loading the URLFilters", e);
            }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url", "content", "metadata"));
        declarer.declareStream(Constants.StatusStreamName, new Fields("url",
                "metadata", "status"));
    }

}
