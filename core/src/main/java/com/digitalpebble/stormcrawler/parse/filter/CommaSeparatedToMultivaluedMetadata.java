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
package com.digitalpebble.stormcrawler.parse.filter;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.parse.ParseFilter;
import com.digitalpebble.stormcrawler.parse.ParseResult;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.jetbrains.annotations.NotNull;
import org.w3c.dom.DocumentFragment;

/**
 * Rewrites single metadata containing comma separated values into multiple values for the same key,
 * useful for instance for keyword tags.
 */
public class CommaSeparatedToMultivaluedMetadata extends ParseFilter {

    private final Set<String> keys = new HashSet<>();

    @Override
    public void configure(@NotNull Map<String, Object> stormConf, @NotNull JsonNode filterParams) {
        JsonNode node = filterParams.get("keys");
        if (node == null) {
            return;
        }
        if (node.isArray()) {
            Iterator<JsonNode> iter = node.iterator();
            while (iter.hasNext()) {
                keys.add(iter.next().asText());
            }
        } else {
            keys.add(node.asText());
        }
    }

    @Override
    public void filter(String url, byte[] content, DocumentFragment doc, ParseResult parse) {
        Metadata m = parse.get(url).getMetadata();
        for (String key : keys) {
            String val = m.getFirstValue(key);
            if (val == null) continue;
            m.remove(key);
            String[] tokens = val.split(" *, *");
            for (String t : tokens) {
                m.addValue(key, t);
            }
        }
    }
}
