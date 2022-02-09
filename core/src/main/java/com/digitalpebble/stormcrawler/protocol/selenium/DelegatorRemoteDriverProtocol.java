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
package com.digitalpebble.stormcrawler.protocol.selenium;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.protocol.Protocol;
import com.digitalpebble.stormcrawler.protocol.ProtocolResponse;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import com.digitalpebble.stormcrawler.util.InitialisationUtil;
import crawlercommons.robots.BaseRobotRules;
import org.apache.storm.Config;
import org.jetbrains.annotations.NotNull;

/**
 * Alternative implementation of RemoteDriverProtocol which delegates the calls to a different
 * implementation if the URL does not have a value for the protocol.use.selenium in its metadata.
 * Allows to use Selenium for some of the URLs only.
 *
 * @deprecated use DelegatorProtocol instead
 */
public class DelegatorRemoteDriverProtocol extends RemoteDriverProtocol {

    private Protocol directProtocol;

    // metadata key value to indicate that we should use Selenium
    // rely on the direct protocol instead
    public static final String USE_SELENIUM_KEY = "protocol.use.selenium";

    public static final String PROTOCOL_IMPL_CONFIG = "selenium.delegated.protocol";

    @Override
    public void configure(@NotNull Config conf) {
        super.configure(conf);
        String protocolimplementation = ConfUtils.getString(conf, PROTOCOL_IMPL_CONFIG);
        try {
            directProtocol =
                    InitialisationUtil.initializeFromQualifiedName(
                            protocolimplementation, Protocol.class);
            directProtocol.configure(conf);
        } catch (Exception e) {
            throw new RuntimeException(
                    "DelegatorRemoteDriverProtocol needs a valid protocol class for the config "
                            + PROTOCOL_IMPL_CONFIG
                            + "but has :"
                            + protocolimplementation,
                    e);
        }
    }

    @Override
    public ProtocolResponse getProtocolOutput(String url, Metadata metadata) throws Exception {
        if (metadata.getFirstValue(USE_SELENIUM_KEY) != null) {
            return super.getProtocolOutput(url, metadata);
        } else {
            return directProtocol.getProtocolOutput(url, metadata);
        }
    }

    @Override
    public void cleanup() {
        super.cleanup();
        directProtocol.cleanup();
    }

    @Override
    public BaseRobotRules getRobotRules(String url) {
        return directProtocol.getRobotRules(url);
    }
}
