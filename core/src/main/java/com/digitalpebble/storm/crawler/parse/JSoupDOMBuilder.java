/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Started from
 * https://svn.apache.org/repos/asf/stanbol/trunk/enhancement-engines/htmlextractor/src/
 * main/java/org/apache/stanbol/enhancer/engines/htmlextractor/impl/DOMBuilder.java
 */
package com.digitalpebble.storm.crawler.parse;

import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.html.dom.HTMLDocumentImpl;
import org.w3c.dom.Document;
import org.w3c.dom.DocumentFragment;
import org.w3c.dom.Node;

/**
 * @author <a href="mailto:kasper@dfki.de">Walter Kasper</a>
 */
public final class JSoupDOMBuilder {

    /**
     * Restrict instantiation
     */
    private JSoupDOMBuilder() {
    }

    /**
     * Returns a W3C DOM that exposes the same content as the supplied Jsoup document into a W3C
     * DOM.
     *
     * @param jsoupDocument The Jsoup document to convert.
     * @return A W3C Document.
     */
    public static Document jsoup2DOM(org.jsoup.nodes.Document jsoupDocument) {

        Document document = null;

        try {

            /* Obtain the document builder for the configured XML parser. */
            DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();

            /* Create a document to contain the content. */
            document = docBuilder.newDocument();
            createDOM(jsoupDocument, document, document, new HashMap<String, String>());

        } catch (ParserConfigurationException pce) {
            throw new RuntimeException(pce);
        }

        return document;
    }

    public static DocumentFragment jsoup2HTML(org.jsoup.nodes.Document jsoupDocument) {
        HTMLDocumentImpl htmlDoc = new HTMLDocumentImpl();
        htmlDoc.setErrorChecking(false);
        DocumentFragment fragment = htmlDoc.createDocumentFragment();
        createDOM(jsoupDocument, fragment, htmlDoc, new HashMap<String, String>());
        return fragment;
    }

    /**
     * The internal helper that copies content from the specified Jsoup <tt>Node</tt> into a W3C
     * {@link Node}.
     *
     * @param node The Jsoup node containing the content to copy to the specified W3C {@link Node}.
     * @param out The W3C {@link Node} that receives the DOM content.
     */
    public static void createDOM(org.jsoup.nodes.Node node, Node out, Document doc,
            Map<String, String> ns) {

        if (node instanceof org.jsoup.nodes.Document) {

            org.jsoup.nodes.Document d = ((org.jsoup.nodes.Document) node);
            for (org.jsoup.nodes.Node n : d.childNodes()) {
                createDOM(n, out, doc, ns);
            }

        } else if (node instanceof org.jsoup.nodes.Element) {

            org.jsoup.nodes.Element e = ((org.jsoup.nodes.Element) node);
            org.w3c.dom.Element _e = doc.createElement(e.tagName());
            out.appendChild(_e);
            org.jsoup.nodes.Attributes atts = e.attributes();

            for (org.jsoup.nodes.Attribute a : atts) {
                String attName = a.getKey();
                // omit xhtml namespace
                if (attName.equals("xmlns")) {
                    continue;
                }
                String attPrefix = getNSPrefix(attName);
                if (attPrefix != null) {
                    if (attPrefix.equals("xmlns")) {
                        ns.put(getLocalName(attName), a.getValue());
                    } else if (!attPrefix.equals("xml")) {
                        String namespace = ns.get(attPrefix);
                        if (namespace == null) {
                            // fix attribute names looking like qnames
                            attName = attName.replace(':', '_');
                        }
                    }
                }
                _e.setAttribute(attName, a.getValue());
            }

            for (org.jsoup.nodes.Node n : e.childNodes()) {
                createDOM(n, _e, doc, ns);
            }

        } else if (node instanceof org.jsoup.nodes.TextNode) {

            org.jsoup.nodes.TextNode t = ((org.jsoup.nodes.TextNode) node);
            if (!(out instanceof Document)) {
                out.appendChild(doc.createTextNode(t.text()));
            }
        }
    }

    // some hacks for handling namespace in jsoup2DOM conversion
    private static String getNSPrefix(String name) {
        if (name != null) {
            int pos = name.indexOf(':');
            if (pos > 0) {
                return name.substring(0, pos);
            }
        }
        return null;
    }

    private static String getLocalName(String name) {
        if (name != null) {
            int pos = name.lastIndexOf(':');
            if (pos > 0) {
                return name.substring(pos + 1);
            }
        }
        return name;
    }
}
