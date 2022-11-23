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

import com.digitalpebble.stormcrawler.util.AbstractConfigurable;
import org.w3c.dom.DocumentFragment;

/**
 * Implementations of ParseFilter are responsible for extracting custom data from the crawled
 * content. They are used by parsing bolts such as {@link
 * com.digitalpebble.stormcrawler.bolt.JSoupParserBolt} or {@link
 * com.digitalpebble.stormcrawler.bolt.SiteMapParserBolt}.
 */
public abstract class ParseFilter extends AbstractConfigurable {

    /**
     * Called when parsing a specific page
     *
     * @param URL the URL of the page being parsed
     * @param content the content being parsed
     * @param doc the DOM tree resulting of the parsing of the content or null if {@link
     *     #needsDOM()} returns <code>false</code>
     * @param parse the metadata to be updated with the resulting of the parsing
     */
    public abstract void filter(
            String URL, byte[] content, DocumentFragment doc, ParseResult parse);

    /**
     * Specifies whether this filter requires a DOM representation of the document
     *
     * @return <code>true</code>if this needs a DOM representation of the document, <code>false
     *     </code> otherwise.
     */
    public boolean needsDOM() {
        return false;
    }
}
