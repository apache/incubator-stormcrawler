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

package com.digitalpebble.storm.crawler.filtering;

import com.digitalpebble.storm.crawler.protocol.MemoryRobotsCache;
import com.digitalpebble.storm.crawler.protocol.RobotsCache;
import com.fasterxml.jackson.databind.JsonNode;
import crawlercommons.robots.BaseRobotRules;

import java.net.MalformedURLException;
import java.net.URL;

/**
 * This {@link com.digitalpebble.storm.crawler.filtering.URLFilter} filters outlinks
 * using the robots rules for the domain. If the rules for the domain aren't found
 * in the cache, the outlink will pass the filter.
 */
public class RobotsURLFilter implements URLFilter {

    private RobotsCache cache;

    public String filter(String URL) {
        try {
            URL url = new URL(URL);
            String key = getCacheKey(url);
            BaseRobotRules rules = cache.get(key);
            // If we have a cache miss, return the URL
            if (rules == null)
                return URL;
            if (rules.isAllowed(URL))
                return URL;
            else
                return null;
        } catch (MalformedURLException e) {
            return null;
        }
    }

    public void configure(JsonNode paramNode) {
        //TODO Specify the cache in the config
        this.cache = MemoryRobotsCache.getInstance();
    }

    /**
     * Compose unique key to store and access robot rules in cache for given URL
     */
    private static String getCacheKey(URL url) {
        // TODO This method is a direct port from HttpRobotsRulesParser. We should consolidate

        String protocol = url.getProtocol().toLowerCase(); // normalize to lower
        // case
        String host = url.getHost().toLowerCase(); // normalize to lower case
        int port = url.getPort();
        if (port == -1) {
            port = url.getDefaultPort();
        }
        /*
         * Robot rules apply only to host, protocol, and port where robots.txt
         * is hosted (cf. NUTCH-1752). Consequently
         */
        String cacheKey = protocol + ":" + host + ":" + port;
        return cacheKey;
    }
}
