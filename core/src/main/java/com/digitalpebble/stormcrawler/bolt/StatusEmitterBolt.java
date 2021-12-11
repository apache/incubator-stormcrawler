/**
 * Licensed to DigitalPebble Ltd under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership.
 * DigitalPebble licenses this file to You under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.digitalpebble.stormcrawler.bolt;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.filtering.URLFilters;
import com.digitalpebble.stormcrawler.parse.Outlink;
import com.digitalpebble.stormcrawler.persistence.Status;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import com.digitalpebble.stormcrawler.util.MetadataTransfer;
import com.digitalpebble.stormcrawler.util.URLUtil;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * Provides common functionalities for Bolts which emit tuples to the status stream, e.g. Fetchers,
 * Parsers. Encapsulates the logic of URL filtering and metadata transfer to outlinks.
 */
public abstract class StatusEmitterBolt extends BaseRichBolt {

    private URLFilters urlFilters;

    private MetadataTransfer metadataTransfer;

    private boolean allowRedirs;

    protected OutputCollector collector;

    @Override
    public void prepare(
            Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        urlFilters = URLFilters.fromConf(stormConf);
        metadataTransfer = MetadataTransfer.getInstance(stormConf);
        allowRedirs =
                ConfUtils.getBoolean(
                        stormConf,
                        com.digitalpebble.stormcrawler.Constants.AllowRedirParamName,
                        true);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(
                com.digitalpebble.stormcrawler.Constants.StatusStreamName,
                new Fields("url", "metadata", "status"));
    }

    /**
     * Used for redirections or when discovering sitemap URLs. The custom key / values are added to
     * the target metadata post-filtering.
     */
    protected void emitOutlink(
            Tuple t, URL sURL, String newUrl, Metadata sourceMetadata, String... customKeyVals) {

        Outlink ol = filterOutlink(sURL, newUrl, sourceMetadata, customKeyVals);
        if (ol == null) return;

        collector.emit(
                com.digitalpebble.stormcrawler.Constants.StatusStreamName,
                t,
                new Values(ol.getTargetURL(), ol.getMetadata(), Status.DISCOVERED));
    }

    protected Outlink filterOutlink(
            URL sURL, String newUrl, Metadata sourceMetadata, String... customKeyVals) {
        // build an absolute URL
        try {
            URL tmpURL = URLUtil.resolveURL(sURL, newUrl);
            newUrl = tmpURL.toExternalForm();
        } catch (MalformedURLException e) {
            return null;
        }

        // apply URL filters
        newUrl = this.urlFilters.filter(sURL, sourceMetadata, newUrl);

        // filtered
        if (newUrl == null) {
            return null;
        }

        Metadata metadata =
                metadataTransfer.getMetaForOutlink(newUrl, sURL.toExternalForm(), sourceMetadata);

        for (int i = 0; i < customKeyVals.length; i = i + 2) {
            metadata.addValue(customKeyVals[i], customKeyVals[i + 1]);
        }

        Outlink l = new Outlink(newUrl);
        l.setMetadata(metadata);
        return l;
    }

    protected boolean allowRedirs() {
        return allowRedirs;
    }
}
