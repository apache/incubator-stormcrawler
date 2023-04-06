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
package com.digitalpebble.stormcrawler.parse;

import com.digitalpebble.stormcrawler.util.ConfUtils;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.nodes.TextNode;
import org.jsoup.select.Elements;
import org.jsoup.select.NodeVisitor;

import static com.digitalpebble.stormcrawler.parse.NodeVisitorTraverser.*;

/**
 * Filters the text extracted from HTML documents, used by JSoupParserBolt. Configured with optional
 * inclusion patterns based on <a href="https://jsoup.org/cookbook/extracting-data/selector-syntax">
 * JSoup selectors</a>, as well as a list of tags to be excluded.
 *
 * <p>Replaces ContentFilter.
 *
 * <p>The first matching inclusion pattern is used or the whole document if no expressions are
 * configured or no match has been found.
 *
 * <p>The TextExtraction can be configured as so:
 *
 * <pre>{@code
 * textextractor.include.pattern:
 *  - DIV[id="maincontent"]
 *  - DIV[itemprop="articleBody"]
 *  - ARTICLE
 *
 * textextractor.exclude.tags:
 *  - STYLE
 *  - SCRIPT
 *
 * }</pre>
 *
 * @since 1.13
 */
public class TextExtractor {

    public static final String INCLUDE_PARAM_NAME = "textextractor.include.pattern";
    public static final String EXCLUDE_PARAM_NAME = "textextractor.exclude.tags";
    public static final String NO_TEXT_PARAM_NAME = "textextractor.no.text";
    public static final String TEXT_MAX_TEXT_PARAM_NAME = "textextractor.skip.after";

    private final List<String> inclusionPatterns;
    private final HashSet<String> excludedTags;
    private final boolean noText;
    private final int maxTextSize;

    public TextExtractor(Map<String, Object> stormConf) {
        maxTextSize = ConfUtils.getInt(stormConf, TEXT_MAX_TEXT_PARAM_NAME, -1);
        noText = ConfUtils.getBoolean(stormConf, NO_TEXT_PARAM_NAME, false);
        inclusionPatterns = ConfUtils.loadListFromConf(INCLUDE_PARAM_NAME, stormConf);
        excludedTags = new HashSet<String>();
        ConfUtils.loadListFromConf(EXCLUDE_PARAM_NAME, stormConf)
                .forEach((s) -> excludedTags.add(s.toLowerCase()));
    }

    public String text(Element element) {
        // not interested in getting any text?
        if (noText) return "";

        final StringBuilder accum = new StringBuilder();

        // no patterns at all - return the text from the whole document
        if (inclusionPatterns.size() == 0 && excludedTags.size() == 0) {
            _text(element, accum);
        } else {
            Elements matches = new Elements();

            for (String pattern : inclusionPatterns) {
                matches = element.select(pattern);
                if (!matches.isEmpty()) {
                    break;
                }
            }

            // if nothing matches or no patterns were defined use the whole doc
            if (matches.isEmpty()) {
                matches.add(element);
            }

            for (Element node : matches) {
                _text(node, accum);
                accum.append("\n");
            }
        }

        return accum.toString().trim();
    }

    private void _text(Node node, final StringBuilder accum) {
        traverse(
                new NodeVisitor() {

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
                                    && (element.isBlock() || element.tag().getName().equals("br"))
                                    && !lastCharIsWhitespace(accum)) accum.append(' ');
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
                                    && !lastCharIsWhitespace(accum)) accum.append(' ');
                        }
                    }
                },
                node,
                maxTextSize,
                accum);
    }



}
