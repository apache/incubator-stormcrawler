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

package com.digitalpebble.storm.crawler.protocol;

import org.apache.storm.guava.cache.Cache;
import org.apache.storm.guava.cache.CacheBuilder;
import org.apache.storm.guava.cache.CacheLoader;
import org.apache.storm.guava.cache.LoadingCache;
import crawlercommons.robots.BaseRobotRules;

import java.net.URL;

/**
 * Provides an in-memory, singleton, thread-safe cache for robots rules.
 */
public class MemoryRobotsCache implements RobotsCache {

    private static final long MAX_SIZE = 1000;

    private static final MemoryRobotsCache INSTANCE = new MemoryRobotsCache();

    private static Cache<String, BaseRobotRules> CACHE;

    public static MemoryRobotsCache getInstance() {
        return INSTANCE;
    }

    private MemoryRobotsCache() {
        CACHE = CacheBuilder.newBuilder().maximumSize(MAX_SIZE).build();
    }

    public BaseRobotRules get(String key) {
        return CACHE.getIfPresent(key);
    }

    public void put(String key, BaseRobotRules rules) {
        CACHE.put(key, rules);
    }

    /**
     * Compose unique key to store and access robot rules in cache for given URL
     */
    public String getCacheKey(URL url) {
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
