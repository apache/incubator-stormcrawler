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

import crawlercommons.robots.BaseRobotRules;

import java.net.URL;

/**
 * This interface defines the methods that must be implemented by a cache for Robots rules.
 */
public interface RobotsCache {

    /**
     *
     * @param key Cache key
     * @return Returns the robots rules for the key, or null if there's a cache miss.
     */
    public BaseRobotRules get(String key);

    /**
     *
     * @param key Cache key
     * @param rules Robots rules to associate with the key
     */
    public void put(String key, BaseRobotRules rules);

    public String getCacheKey(URL url);
}
