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
import org.apache.storm.Config;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.Timeout;
import org.openqa.selenium.chrome.ChromeOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BrowserWebDriverContainer;
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
            new BrowserWebDriverContainer<>(IMAGE).withCapabilities(new ChromeOptions());

    @Before
    public void setupProtocol() {

        LOG.info(
                "Configuring protocol instance to connect to {}",
                chrome.getSeleniumAddress().toExternalForm());

        Config conf = new Config();
        conf.put("http.agent.name", "this.is.only.a.test");
        conf.put("selenium.addresses", chrome.getSeleniumAddress().toExternalForm());
        conf.put("selenium.setScriptTimeout", 10000);
        conf.put("selenium.pageLoadTimeout", 1000);
        conf.put("selenium.implicitlyWait", 1000);

        // TODO add the following
        // selenium.capabilities:
        // chromeOptions:
        // args:
        // - "--no-sandbox"
        // - "--disable-dev-shm-usage"
        // - "--headless"
        // - "--disable-gpu"
        // goog:chromeOptions:
        // args:
        // - "--no-sandbox"
        // - "--disable-dev-shm-usage"
        // - "--headless"
        // - "--disable-gpu"

        protocol = new RemoteDriverProtocol();
        protocol.configure(conf);
    }

    // @Test
    // not working yet
    public void test() {
        Metadata m = new Metadata();
        try {
            ProtocolResponse response = protocol.getProtocolOutput("http://digitalpebble.com", m);
            response.getMetadata();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @After
    public void close() {
        protocol.cleanup();
    }
}
