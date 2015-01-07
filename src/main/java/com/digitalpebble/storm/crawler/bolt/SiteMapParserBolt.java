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

import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.digitalpebble.storm.crawler.Constants;
import com.digitalpebble.storm.crawler.persistence.Status;
import com.digitalpebble.storm.crawler.protocol.HttpHeaders;
import com.digitalpebble.storm.crawler.util.KeyValues;

import crawlercommons.sitemaps.AbstractSiteMap;
import crawlercommons.sitemaps.SiteMap;
import crawlercommons.sitemaps.SiteMapIndex;
import crawlercommons.sitemaps.SiteMapURL;
import crawlercommons.sitemaps.SiteMapURL.ChangeFrequency;

/**
 * Extracts URLs from sitemap files. The parsing is triggered by the presence of
 * 'isSitemap=true' in the metadata. Any tuple which does not have this
 * key/value in the metadata is simply passed on to the default stream, whereas
 * any URLs extracted from the sitemaps is sent to the 'status' field.
 **/
public class SiteMapParserBolt extends BaseRichBolt {

    private OutputCollector collector;
    public static final String isSitemapKey = "isSitemap";

    private boolean strictMode = false;

    private static final org.slf4j.Logger LOG = LoggerFactory
            .getLogger(SiteMapParserBolt.class);

    @Override
    public void execute(Tuple tuple) {
        HashMap<String, String[]> metadata = (HashMap<String, String[]>) tuple
                .getValueByField("metadata");

        // TODO check that we have the right number of fields ?
        String isSitemap = KeyValues.getValue(isSitemapKey, metadata);
        if (!Boolean.valueOf(isSitemap)) {
            // just pass it on
            this.collector.emit(tuple.getValues());
            this.collector.ack(tuple);
            return;
        }

        // it does have the right key/value
        byte[] content = tuple.getBinaryByField("content");
        String url = tuple.getStringByField("url");
        String ct = KeyValues.getValue(HttpHeaders.CONTENT_TYPE, metadata);
        List<Values> outlinks = parseSiteMap(url, content, ct);

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
            String contentType) {

        crawlercommons.sitemaps.SiteMapParser parser = new crawlercommons.sitemaps.SiteMapParser(
                strictMode);

        AbstractSiteMap siteMap = null;
        try {
            siteMap = parser.parseSiteMap(contentType, content, new URL(url));
        } catch (Exception e) {
            LOG.error("Exception while parsing sitemap", e);
            return Collections.emptyList();
        }

        List<Values> links = new ArrayList<Values>();

        if (siteMap.isIndex()) {
            SiteMapIndex smi = ((SiteMapIndex) siteMap);
            Collection<AbstractSiteMap> subsitemaps = smi.getSitemaps();
            // keep the subsitemaps as outlinks
            // they will be fetched and parsed in the following steps
            Iterator<AbstractSiteMap> iter = subsitemaps.iterator();
            while (iter.hasNext()) {
                String s = iter.next().getUrl().toExternalForm();
                // TODO apply filtering to outlinks
                // TODO configure which metadata gets inherited from parent
                HashMap<String, String[]> metadata = KeyValues.newInstance();
                KeyValues.setValue(isSitemapKey, metadata, "true");
                Values ol = new Values(s, metadata, Status.DISCOVERED);
                links.add(ol);
                LOG.debug("{} : [sitemap] {}", url, s);
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
                // TODO configure which metadata gets inherited from parent
                String s = smurl.getUrl().toExternalForm();
                // TODO apply filtering to outlinks
                HashMap<String, String[]> metadata = KeyValues.newInstance();
                KeyValues.setValue(isSitemapKey, metadata, "false");
                Values ol = new Values(s, metadata, Status.DISCOVERED);
                links.add(ol);
                LOG.debug("{} : [sitemap] {}", url, s);
            }
        }

        return links;
    }

    @Override
    public void prepare(Map conf, TopologyContext context,
            OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url", "content", "metadata"));
        declarer.declareStream(Constants.StatusStreamName, new Fields("url",
                "metadata", "status"));
    }

}
