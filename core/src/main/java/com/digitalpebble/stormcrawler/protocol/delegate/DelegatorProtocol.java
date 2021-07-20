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

package com.digitalpebble.stormcrawler.protocol.delegate;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.protocol.Protocol;
import com.digitalpebble.stormcrawler.protocol.ProtocolResponse;
import crawlercommons.robots.BaseRobotRules;
import org.apache.storm.Config;

/**
 * Protocol implementation that enables selection from a collection of sub-protocols using filters based on each call's
 * target url and metadata
 **/
public class DelegatorProtocol implements Protocol {
    private DelegationFilters protocolFilters;

    @Override
    public void configure(Config conf) {
        // load delegation filters from the passed configuration
        protocolFilters = DelegationFilters.fromConf(conf);
    }

    @Override
    public ProtocolResponse getProtocolOutput(String url, Metadata metadata)
            throws Exception {
        // route robots.txt urls to the robots protocol explicitly
        if (url.endsWith("/robots.txt"))
            return protocolFilters.getRobotsProtocol().getProtocolOutput(url, metadata);

        // retrieve the protocol that should be used for this url-meta combo
        Protocol proto = protocolFilters.getProtocol(url, metadata);

        // execute and return protocol with url-meta combo
        return proto.getProtocolOutput(url, metadata);
    }

    @Override
    public void cleanup() {
        protocolFilters.cleanup();
    }

    @Override
    public BaseRobotRules getRobotRules(String url) {
        return protocolFilters.getRobotsProtocol().getRobotRules(url);
    }
}
