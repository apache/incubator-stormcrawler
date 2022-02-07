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
package com.digitalpebble.stormcrawler.jsoup;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.filtering.URLFilters;
import com.digitalpebble.stormcrawler.parse.JSoupFilter;
import com.digitalpebble.stormcrawler.parse.Outlink;
import com.digitalpebble.stormcrawler.parse.ParseData;
import com.digitalpebble.stormcrawler.parse.ParseResult;
import com.digitalpebble.stormcrawler.util.MetadataTransfer;
import com.digitalpebble.stormcrawler.util.URLUtil;
import com.fasterxml.jackson.databind.JsonNode;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import org.jsoup.select.Evaluator;
import org.jsoup.select.QueryParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ParseFilter to extract additional links with CSS expressions can be configured with e.g.
 *
 * <pre>{@code
 * {
 *   "class": "com.digitalpebble.stormcrawler.jsoup.LinkParseFilter",
 *   "name": "LinkParseFilter",
 *   "params": {
 *     "pattern": "img[src]",
 *     "pattern2": "video/source[src]"
 *   }
 * }
 *
 * }</pre>
 */
public class LinkParseFilter extends JSoupFilter {

    private static final Logger LOG = LoggerFactory.getLogger(LinkParseFilter.class);

    private MetadataTransfer metadataTransfer;

    private URLFilters urlFilters;

    private List<EvaluatorWrapper> evaluator = new ArrayList<>();

    static class EvaluatorWrapper {
        Evaluator evaluator;
        String attributeName;

        EvaluatorWrapper(Evaluator e, String attributeName) {
            this.evaluator = e;
            this.attributeName = attributeName;
        }
    }

    @Override
    public void filter(String URL, byte[] content, Document doc, ParseResult parse) {

        ParseData parseData = parse.get(URL);
        Metadata metadata = parseData.getMetadata();

        Map<String, Outlink> dedup = new HashMap<String, Outlink>();

        for (Outlink o : parse.getOutlinks()) {
            dedup.put(o.getTargetURL(), o);
        }

        java.net.URL sourceUrl;
        try {
            sourceUrl = new URL(URL);
        } catch (MalformedURLException e1) {
            // we would have known by now as previous components check whether
            // the URL is valid
            LOG.error("MalformedURLException on {}", URL);
            return;
        }

        // applies the expressions in the order in which they are listed
        for (EvaluatorWrapper eval : evaluator) {

            Elements elements = doc.select(eval.evaluator);
            List<String> results = null;
            if (eval.attributeName == null) {
                results = elements.eachText();
            } else {
                results = elements.eachAttr(eval.attributeName);
            }

            for (String target : results) {
                // resolve URL
                try {
                    target = URLUtil.resolveURL(sourceUrl, target).toExternalForm();
                    // apply filtering
                    target = urlFilters.filter(sourceUrl, metadata, target);
                } catch (MalformedURLException e) {
                    // ignore
                }

                if (target == null) {
                    continue;
                }

                // check whether we already have this link
                if (dedup.containsKey(target)) {
                    continue;
                }

                // create outlink
                Outlink ol = new Outlink(target);

                // get the metadata for the outlink from the parent one
                Metadata metadataOL = metadataTransfer.getMetaForOutlink(target, URL, metadata);

                ol.setMetadata(metadataOL);
                dedup.put(ol.getTargetURL(), ol);
            }
        }

        parse.setOutlinks(new ArrayList(dedup.values()));
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void configure(Map stormConf, JsonNode filterParams) {
        super.configure(stormConf, filterParams);
        this.metadataTransfer = MetadataTransfer.getInstance(stormConf);
        this.urlFilters = URLFilters.fromConf(stormConf);

        java.util.Iterator<Entry<String, JsonNode>> iter = filterParams.fields();
        while (iter.hasNext()) {
            Entry<String, JsonNode> entry = iter.next();
            JsonNode node = entry.getValue();
            String text = node.asText();
            Evaluator e = QueryParser.parse(text);
            Pattern pat = Pattern.compile(".+\\[(.+)\\]$");
            Matcher m = pat.matcher(text);
            String attr = null;
            m.find();
            if (m.matches()) {
                attr = m.group(1);
            }
            evaluator.add(new EvaluatorWrapper(e, attr));
        }
    }
}
