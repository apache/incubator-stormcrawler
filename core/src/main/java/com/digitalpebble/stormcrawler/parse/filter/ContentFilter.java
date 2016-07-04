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

package com.digitalpebble.stormcrawler.parse.filter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.digitalpebble.stormcrawler.parse.ParseData;
import com.digitalpebble.stormcrawler.parse.ParseFilter;
import com.digitalpebble.stormcrawler.parse.ParseResult;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Restricts the text of the main document based on the text value of an Xpath
 * expression (e.g. &lt;div id='maincontent'&gt;). This is useful when dealing
 * with a known format to get rid of the boilerplate HTML code.
 **/
public class ContentFilter extends ParseFilter {

    private static final Logger LOG = LoggerFactory
            .getLogger(ContentFilter.class);

    private XPath xpath = XPathFactory.newInstance().newXPath();
    private List<XPathExpression> expressions;

    @Override
    public void filter(String URL, byte[] content, DocumentFragment doc,
            ParseResult parse) {

        ParseData pd = parse.get(URL);

        // TODO determine how to restrict the expressions e.g. regexp on URL
        // or value in metadata

        // iterates on the expressions - stops at the first that matches
        for (XPathExpression expression : expressions) {
            try {
                NodeList evalResults = (NodeList) expression.evaluate(doc,
                        XPathConstants.NODESET);
                if (evalResults.getLength() == 0) {
                    continue;
                }
                StringBuilder newText = new StringBuilder();
                for (int i = 0; i < evalResults.getLength(); i++) {
                    Node node = evalResults.item(i);
                    newText.append(node.getTextContent()).append("\n");
                }

                // ignore if no text captured
                if (StringUtils.isBlank(newText.toString())) {
                    LOG.debug(
                            "Found match for doc {} but empty text extracted - skipping",
                            URL);
                    continue;
                }

                // give the doc its new text value
                LOG.debug(
                        "Restricted text for doc {}. Text size was {} and is now {}",
                        URL, pd.getText().length(), newText.length());

                pd.setText(newText.toString());

                return;
            } catch (XPathExpressionException e) {
                LOG.error("Caught XPath expression", e);
            }
        }

    }

    @SuppressWarnings("rawtypes")
    @Override
    public void configure(Map stormConf, JsonNode filterParams) {
        expressions = new ArrayList<>();
        java.util.Iterator<Entry<String, JsonNode>> iter = filterParams
                .fields();
        while (iter.hasNext()) {
            Entry<String, JsonNode> entry = iter.next();
            String key = entry.getKey();
            String xpathvalue = entry.getValue().asText();
            try {
                expressions.add(xpath.compile(xpathvalue));
            } catch (XPathExpressionException e) {
                throw new RuntimeException("Can't compile expression : "
                        + xpathvalue, e);
            }
        }
    }

    @Override
    public boolean needsDOM() {
        return true;
    }

}
