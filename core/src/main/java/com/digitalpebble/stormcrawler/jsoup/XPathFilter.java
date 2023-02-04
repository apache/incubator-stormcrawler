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
import com.digitalpebble.stormcrawler.parse.JSoupFilter;
import com.digitalpebble.stormcrawler.parse.ParseData;
import com.digitalpebble.stormcrawler.parse.ParseResult;
import com.digitalpebble.stormcrawler.util.AbstractConfigurable;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.xsoup.XPathEvaluator;
import us.codecraft.xsoup.Xsoup;

/** Reads a XPATH patterns and stores the value found in web page as metadata */
public class XPathFilter extends AbstractConfigurable implements JSoupFilter {

    private static final Logger LOG = LoggerFactory.getLogger(XPathFilter.class);

    protected final Map<String, List<LabelledExpression>> expressions = new HashMap<>();

    static class LabelledExpression {

        String key;

        private XPathEvaluator expression;
        private String xpath;

        private LabelledExpression(String key, String xpath) {
            this.key = key;
            this.xpath = xpath;
            this.expression = Xsoup.compile(xpath);
        }

        List<String> evaluate(org.jsoup.nodes.Document doc) throws IOException {
            return expression.evaluate(doc).list();
        }

        public String toString() {
            return key + ":" + xpath;
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void configure(@NotNull Map stormConf, @NotNull JsonNode filterParams) {
        super.configure(stormConf, filterParams);
        java.util.Iterator<Entry<String, JsonNode>> iter = filterParams.fields();
        while (iter.hasNext()) {
            Entry<String, JsonNode> entry = iter.next();
            String key = entry.getKey();
            JsonNode node = entry.getValue();
            if (node.isArray()) {
                for (JsonNode expression : node) {
                    addExpression(key, expression);
                }
            } else {
                addExpression(key, entry.getValue());
            }
        }
    }

    private void addExpression(String key, JsonNode expression) {
        String xpathvalue = expression.asText();
        try {
            expressions
                    .computeIfAbsent(key, k -> new ArrayList<>())
                    .add(new LabelledExpression(key, xpathvalue));
        } catch (Exception e) {
            throw new RuntimeException("Can't compile expression : " + xpathvalue, e);
        }
    }

    @Override
    public void filter(
            String URL, byte[] content, org.jsoup.nodes.Document doc, ParseResult parse) {

        ParseData parseData = parse.get(URL);
        Metadata metadata = parseData.getMetadata();

        // applies the XPATH expression in the order in which they are produced
        java.util.Iterator<List<LabelledExpression>> iter = expressions.values().iterator();
        while (iter.hasNext()) {
            List<LabelledExpression> leList = iter.next();
            for (LabelledExpression le : leList) {
                try {
                    List<String> values = le.evaluate(doc);
                    if (values != null && !values.isEmpty()) {
                        metadata.addValues(le.key, values);
                        break;
                    }
                } catch (IOException e) {
                    LOG.error("Error evaluating {}: {}", le.key, e);
                }
            }
        }
    }
}
