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
package com.digitalpebble.stormcrawler.parse;

import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.jsoup.helper.StringUtil;
import org.jsoup.nodes.CDataNode;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.nodes.TextNode;
import org.jsoup.select.Elements;
import org.jsoup.select.NodeTraversor;
import org.jsoup.select.NodeVisitor;

import com.digitalpebble.stormcrawler.util.ConfUtils;

/**
 * Filters the text extracted from HTML documents, used by JSoupParserBolt.
 * Configured with optional inclusion patterns based on <a
 * href="https://jsoup.org/cookbook/extracting-data/selector-syntax"> JSoup
 * selectors</a>, as well as a list of tags to be excluded.
 * 
 * Replaces {@link ContentFilter}.
 * 
 * The first matching inclusion pattern is used or the whole document if no
 * expressions are configured or no match has been found.
 * 
 * The TextExtraction can be configured as so:
 * 
 * <pre>
 * {@code 
 *   textextractor.include.pattern:
 *    - DIV[id="maincontent"]
 *    - DIV[itemprop="articleBody"]
 *    - ARTICLE
 * 
 *   textextractor.exclude.tags:
 *    - STYLE
 *    - SCRIPT
 *    }
 * </pre>
 * 
 * @since 1.13
 **/
public class TextExtractor {

    public final static String INCLUDE_PARAM_NAME = "textextractor.include.pattern";
    public final static String EXCLUDE_PARAM_NAME = "textextractor.exclude.tags";

    private List<String> inclusionPatterns;
    private HashSet<String> excludedTags;

    public TextExtractor(Map stormConf) {
        inclusionPatterns = ConfUtils.loadListFromConf(INCLUDE_PARAM_NAME,
                stormConf);
        excludedTags = new HashSet<String>();
        ConfUtils.loadListFromConf(EXCLUDE_PARAM_NAME, stormConf)
                .forEach((s) -> excludedTags.add(s.toLowerCase()));
    }

    public String text(Element element) {
        // no patterns at all - return the text from the whole document
        if (inclusionPatterns.size() == 0 && excludedTags.size() == 0) {
            return _text(element);
        }

        Elements matches = new Elements();

        for (String pattern : inclusionPatterns) {
            matches = element.select(pattern);
            if (!matches.isEmpty())
                break;
        }

        // if nothing matches or no patterns were defined use the whole doc
        if (matches.isEmpty()) {
            matches.add(element);
        }

        final StringBuilder accum = new StringBuilder();

        for (Element node : matches) {
            accum.append(_text(node)).append("\n");
        }

        return accum.toString().trim();
    }

    private String _text(Node node) {
        final StringBuilder accum = new StringBuilder();
        NodeTraversor.traverse(new NodeVisitor() {

            private Node excluded = null;

            public void head(Node node, int depth) {
                if (excluded == null && node instanceof TextNode) {
                    TextNode textNode = (TextNode) node;
                    appendNormalisedText(accum, textNode);
                } else if (node instanceof Element) {
                    Element element = (Element) node;
                    if (excludedTags.contains(element.tagName())) {
                        excluded = element;
                    }
                    if (accum.length() > 0
                            && (element.isBlock() || element.tag().getName()
                                    .equals("br"))
                            && !lastCharIsWhitespace(accum))
                        accum.append(' ');
                }
            }

            public void tail(Node node, int depth) {
                // make sure there is a space between block tags and immediately
                // following text nodes <div>One</div>Two should be "One Two".
                if (node instanceof Element) {
                    Element element = (Element) node;
                    if (element == excluded) {
                        excluded = null;
                    }
                    if (element.isBlock()
                            && (node.nextSibling() instanceof TextNode)
                            && !lastCharIsWhitespace(accum))
                        accum.append(' ');
                }

            }
        }, node);
        return accum.toString().trim();
    }

    private static void appendNormalisedText(StringBuilder accum,
            TextNode textNode) {
        String text = textNode.getWholeText();

        if (preserveWhitespace(textNode.parent())
                || textNode instanceof CDataNode)
            accum.append(text);
        else
            StringUtil.appendNormalisedWhitespace(accum, text,
                    lastCharIsWhitespace(accum));
    }

    static boolean preserveWhitespace(Node node) {
        // looks only at this element and five levels up, to prevent recursion &
        // needless stack searches
        if (node != null && node instanceof Element) {
            Element el = (Element) node;
            int i = 0;
            do {
                if (el.tag().preserveWhitespace())
                    return true;
                el = el.parent();
                i++;
            } while (i < 6 && el != null);
        }
        return false;
    }

    static boolean lastCharIsWhitespace(StringBuilder sb) {
        return sb.length() != 0 && sb.charAt(sb.length() - 1) == ' ';
    }

}
