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

import backtype.storm.Config;

import com.digitalpebble.storm.crawler.Metadata;

import crawlercommons.robots.BaseRobotRules;

public interface Protocol {

    public void configure(Config conf);

    /**
     * Fetches the content and additional metadata
     * 
     * IMPORTANT: the metadata returned within the response should only be new
     * <i>additional</i>, no need to return the metadata passed in.
     * 
     * @param url
     *            the location of the content
     * @param metadata
     *            extra information
     * @return the content and optional metadata fetched via this protocol
     * @throws Exception
     */
    public ProtocolResponse getProtocolOutput(String url, Metadata metadata)
            throws Exception;

    public BaseRobotRules getRobotRules(String url);
}
