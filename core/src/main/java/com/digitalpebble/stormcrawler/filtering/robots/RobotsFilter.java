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
package com.digitalpebble.stormcrawler.filtering.robots;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.filtering.URLFilter;
import com.digitalpebble.stormcrawler.protocol.HttpRobotRulesParser;
import com.digitalpebble.stormcrawler.protocol.ProtocolFactory;
import com.fasterxml.jackson.databind.JsonNode;
import crawlercommons.robots.BaseRobotRules;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import org.apache.storm.Config;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * URLFilter which discards URLs based on the robots.txt directives. This is meant to be used on
 * small, limited crawls where the number of hosts is finite. Using this on a larger or open crawl
 * would have a negative impact on performance as the filter would try to retrieve the robots.txt
 * files for any host found, unless fromCacheOnly is set to true, in which case the performance will
 * be preserved at the cost of coverage. <br>
 * The filter is configured like so"
 *
 * <pre>
 *  {
 *    "class": "com.digitalpebble.stormcrawler.filtering.robots.RobotsFilter",
 *    "name": "RobotsFilter",
 *    "params": {
 *      "fromCacheOnly": true
 *    }
 *  }
 * </pre>
 */
public class RobotsFilter extends URLFilter {

    private com.digitalpebble.stormcrawler.protocol.HttpRobotRulesParser robots;
    private ProtocolFactory factory;
    private boolean fromCacheOnly = true;

    @Override
    public @Nullable String filter(
            @Nullable URL sourceUrl,
            @Nullable Metadata sourceMetadata,
            @NotNull String urlToFilter) {
        URL target;
        try {
            target = new URL(urlToFilter);
        } catch (MalformedURLException e) {
            return null;
        }

        BaseRobotRules rules;

        if (fromCacheOnly) {
            rules = robots.getRobotRulesSetFromCache(target);
        } else {
            rules = robots.getRobotRulesSet(factory.getProtocol(target), target);
        }

        if (!rules.isAllowed(urlToFilter)) {
            return null;
        }
        return urlToFilter;
    }

    @Override
    public void configure(@NotNull Map<String, Object> stormConf, @NotNull JsonNode filterParams) {
        Config conf = new Config();
        conf.putAll(stormConf);
        factory = ProtocolFactory.getInstance(conf);
        robots = new HttpRobotRulesParser(conf);

        JsonNode node = filterParams.get("fromCacheOnly");
        if (node != null && node.isBoolean()) {
            fromCacheOnly = node.booleanValue();
        }
    }
}
