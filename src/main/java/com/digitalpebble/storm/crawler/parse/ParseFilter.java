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

package com.digitalpebble.storm.crawler.parse;

import java.util.HashMap;

import org.w3c.dom.DocumentFragment;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Implementations of ParseFilter are called by the ParserBolt to extract custom
 * data from webpages
 **/
public interface ParseFilter {

    public void filter(String URL, byte[] content, DocumentFragment doc,
            HashMap<String, String[]> metadata);

    /** Configuration of the filter with a JSONNode object **/
    public void configure(JsonNode paramNode);

    /**
     * Returns true if the ParseFilter needs a DOM representation of the
     * document, false otherwise.
     **/
    public boolean needsDOM();

}
