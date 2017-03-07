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

package com.digitalpebble.stormcrawler.filtering.basic;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.filtering.URLFilter;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Simple URL filters : can be used early in the filtering chain
 **/
public class BasicURLFilter implements URLFilter {

    private int maxPathRepetition = 3;
    private int maxLength = -1;

    public String filter(URL sourceUrl, Metadata sourceMetadata,
            String urlToFilter) {

        if (urlToFilter == null) {
            return null;
        }

        if (maxLength > 0 && urlToFilter.length() > maxLength) {
            return null;
        }
        if (maxPathRepetition > 1) {
            urlToFilter = filterPathRepet(urlToFilter);
        }
        return urlToFilter;
    }

    public final String filterPathRepet(String urlToFilter) {
        // check whether a path element is repeated N times
        String[] paths = urlToFilter.split("/");
        if (paths.length <= 4)
            return urlToFilter;

        Map<String, Integer> count = new HashMap<>();
        for (String s : paths) {
            if (s.length() == 0) {
                continue;
            }
            Integer c = count.get(s);
            if (c == null) {
                c = 1;
            } else {
                c = c + 1;
                if (c == maxPathRepetition) {
                    return null;
                }
            }
            count.put(s, c);
        }

        return urlToFilter;
    }

    @Override
    public void configure(Map stormConf, JsonNode filterParams) {
        JsonNode repet = filterParams.get("maxPathRepetition");
        if (repet != null) {
            maxPathRepetition = repet.asInt(3);
        }

        JsonNode length = filterParams.get("maxLength");
        if (length != null) {
            maxLength = length.asInt(-1);
        }
    }

}
