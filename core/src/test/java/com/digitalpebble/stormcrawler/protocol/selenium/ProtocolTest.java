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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.Config;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.openqa.selenium.chrome.ChromeOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BrowserWebDriverContainer;
import org.testcontainers.containers.BrowserWebDriverContainer.VncRecordingMode;
import org.testcontainers.utility.DockerImageName;

/**
 * Tests the Selenium protocol implementation on a standalone Chrome instance and not through
 * Selenium Grid. https://java.testcontainers.org/modules/webdriver_containers/#example
 */
public class ProtocolTest {

    @Rule public Timeout globalTimeout = Timeout.seconds(120);

    private static final Logger LOG = LoggerFactory.getLogger(ProtocolTest.class);

    private Protocol protocol;

    private static final DockerImageName IMAGE =
            DockerImageName.parse("selenium/standalone-chrome:116.0");

    @Rule
    public BrowserWebDriverContainer<?> chrome =
            new BrowserWebDriverContainer<>(IMAGE)
                    .withCapabilities(new ChromeOptions())
                    .withRecordingMode(VncRecordingMode.SKIP, null);

    @Before
    public void setupProtocol() {

        LOG.info(
                "Configuring protocol instance to connect to {}",
                chrome.getSeleniumAddress().toExternalForm());

        List<String> l = new ArrayList<>();
        // l.add("--no-sandbox");
        // l.add("--disable-dev-shm-usage");
        // l.add("--headless");
        // l.add("--disable-gpu");
        // l.add("--remote-allow-origins=*");
        Map<String, Object> m = new HashMap<>();
        m.put("args", l);
        // m.put("extensions", Collections.EMPTY_LIST);

        Map<String, Object> capabilities = new HashMap<>();
        capabilities.put("browserName", "chrome");
        capabilities.put("goog:chromeOptions", m);

        Config conf = new Config();
        conf.put("http.agent.name", "this.is.only.a.test");
        conf.put("selenium.addresses", chrome.getSeleniumAddress().toExternalForm());

        Map<String, Object> timeouts = new HashMap<>();

        timeouts.put("implicit", 10000);
        timeouts.put("pageLoad", 10000);
        timeouts.put("script", 10000);

        conf.put("selenium.timeouts", timeouts);

        conf.put("selenium.capabilities", capabilities);

        protocol = new RemoteDriverProtocol();
        protocol.configure(conf);
    }

    @Test
    // not working yet
    public void test() {
        Metadata m = new Metadata();
        boolean noException = true;
        try {
            // find better examples later
            ProtocolResponse response = protocol.getProtocolOutput("https://stormcrawler.net", m);
            Assert.assertEquals(307, response.getStatusCode());
        } catch (Exception e) {
            noException = false;
            LOG.info("Exception caught", e);
        }
        Assert.assertEquals(true, noException);
    }

    @After
    public void close() {
        protocol.cleanup();
    }
}
