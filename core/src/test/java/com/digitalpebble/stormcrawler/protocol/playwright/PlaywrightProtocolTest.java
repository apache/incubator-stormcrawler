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

package com.digitalpebble.stormcrawler.protocol.playwright;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.protocol.ProtocolResponse;
import org.apache.storm.Config;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class PlaywrightProtocolTest {
    @Test
    public void testConstruct() {
        Config config = new Config();
        config.put("http.agent.name", "test");

        PlaywrightProtocol proto = new PlaywrightProtocol();
        proto.configure(config);

        config.put("playwright.browser.name", "chrome");
        config.put("playwright.agent.name", "test");
        config.put("playwright.browser.width", 2640);
        config.put("playwright.browser.height", 1440);
        config.put("playwright.timeout", 3000);
        config.put("playwright.content.limit", 64*1024);
        config.put("playwright.instances.num", 4);
        config.put("playwright.headless", false);

        proto = new PlaywrightProtocol();
        proto.configure(config);

        config.put("playwright.browser.name", "firefox");

        proto = new PlaywrightProtocol();
        proto.configure(config);

        config.put("playwright.browser.name", "safari");

        proto = new PlaywrightProtocol();
        proto.configure(config);
    }

    @Test
    public void testGetProtocolOutput() {
        Config config = new Config();
        config.put("http.agent.name", "test");

        config.put("playwright.browser.name", "chrome");
        config.put("playwright.agent.name", "test");
        config.put("playwright.browser.width", 2640);
        config.put("playwright.browser.height", 1440);
        config.put("playwright.timeout", 3000);
        config.put("playwright.content.limit", 64*1024);
        config.put("playwright.instances.num", 4);

        PlaywrightProtocol proto = new PlaywrightProtocol();
        proto.configure(config);

        ProtocolResponse response = proto.getProtocolOutput("https://digitalpebble.com/", new Metadata());

        Assert.assertEquals(200, response.getStatusCode());
        Assert.assertEquals(64*1024, response.getContent().length);

        config.put("playwright.headless", false);

        proto = new PlaywrightProtocol();
        proto.configure(config);

        response = proto.getProtocolOutput("https://digitalpebble.com/", new Metadata());

        Assert.assertEquals(200, response.getStatusCode());
        Assert.assertEquals(64*1024, response.getContent().length);
    }
}
