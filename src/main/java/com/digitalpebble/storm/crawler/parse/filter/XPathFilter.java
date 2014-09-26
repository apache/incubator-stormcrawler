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

package com.digitalpebble.storm.crawler.parse.filter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.JsonNode;
import org.w3c.dom.DocumentFragment;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import ucar.ma2.Range.Iterator;

import com.digitalpebble.storm.crawler.parse.ParseFilter;
import com.digitalpebble.storm.crawler.util.KeyValues;

/**
 * Simple ParseFilter to illustrate and test the interface. Reads a XPATH
 * pattern from the config file and stores the value as metadata
 **/

public class XPathFilter implements ParseFilter {

    public static final Log LOG = LogFactory.getLog(XPathFilter.class);

    private static XPathFactory factory = XPathFactory.newInstance();
    private static XPath xpath = factory.newXPath();

    private List<LabelledExpression> expressions = new ArrayList<LabelledExpression>();

    private class LabelledExpression {

        private String key;
        private XPathExpression expression;

        private LabelledExpression(String key, XPathExpression expression) {
            this.key = key;
            this.expression = expression;
        }
    }

    @Override
    public void filter(String URL, byte[] content, DocumentFragment doc,
            HashMap<String, String[]> metadata) {

        // applies the XPATH expression in the order in which they are produced

        java.util.Iterator<LabelledExpression> iter = expressions.iterator();
        while (iter.hasNext()) {
            LabelledExpression le = iter.next();
            Set<String> values = new HashSet<String>();
            try {
                NodeList nodes = (NodeList) le.expression.evaluate(doc,
                        XPathConstants.NODESET);
                for (int i = 0; i < nodes.getLength(); i++) {
                    Node node = nodes.item(0);
                    values.add(node.getTextContent());
                }
            } catch (XPathExpressionException e) {
                LOG.error(e);
            }
            // add the values to this key
            KeyValues.addValues(le.key, metadata, values);
        }

    }

    @Override
    public void configure(JsonNode paramNode) {
        java.util.Iterator<Entry<String, JsonNode>> iter = paramNode
                .getFields();
        while (iter.hasNext()) {
            Entry<String, JsonNode> entry = iter.next();
            String key = entry.getKey();
            String xpathvalue = entry.getValue().asText();
            try {
                LabelledExpression lexpression = new LabelledExpression(key,
                        xpath.compile(xpathvalue));
                expressions.add(lexpression);
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
