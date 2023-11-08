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

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertNotEquals;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.protocol.ProtocolResponse;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.storm.Config;
import org.apache.storm.utils.MutableObject;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
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

    private static final DockerImageName SELENIUM_IMAGE =
            DockerImageName.parse("selenium/standalone-chrome");

    @Rule
    public BrowserWebDriverContainer<?> chrome =
            new BrowserWebDriverContainer<>(SELENIUM_IMAGE)
                    .withCapabilities(new ChromeOptions())
                    .withRecordingMode(VncRecordingMode.SKIP, null)
                    .withAccessToHost(true)
                    .withExtraHost("website.test", "host-gateway");

    private static Server httpServer;
    private static final Integer HTTP_PORT = findRandomOpenPortOnAllLocalInterfaces();

    @BeforeClass
    public static void initJetty() throws Exception {
        assertNotEquals(Integer.valueOf(-1), HTTP_PORT);
        httpServer = new Server(HTTP_PORT);

        final HandlerList handlers = new HandlerList();
        handlers.setHandlers(new Handler[] {new WildcardResourceHandler()});
        httpServer.setHandler(handlers);
        httpServer.start();
    }

    @AfterClass
    public static void stopJetty() {
        try {
            httpServer.stop();
        } catch (Exception ignored) {

        }
    }

    private static Integer findRandomOpenPortOnAllLocalInterfaces() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            return -1;
        }
    }

    public RemoteDriverProtocol getProtocol() {

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

        RemoteDriverProtocol protocol = new RemoteDriverProtocol();
        protocol.configure(conf);
        return protocol;
    }

    /**
     * you can configure one instance of Selenium to talk to multiple drivers but can't have a
     * multiple instances of the protocol. If there is only one instance and one target, you must
     * wait...
     */
    @Test
    public void testBlocking() {

        RemoteDriverProtocol protocol = getProtocol();

        MutableBoolean noException = new MutableBoolean(true);

        MutableObject endTimeFirst = new MutableObject();
        MutableObject startTimeSecond = new MutableObject();

        await().until(() -> httpServer.isRunning());

        final String url = "http://website.test" + ":" + HTTP_PORT + "/";

        new Thread(
                        () -> {
                            try {
                                ProtocolResponse response =
                                        protocol.getProtocolOutput(url, new Metadata());
                                endTimeFirst.setObject(
                                        Instant.parse(
                                                response.getMetadata()
                                                        .getFirstValue(
                                                                SeleniumProtocol.MD_KEY_END)));
                            } catch (Exception e) {
                                noException.setValue(false);
                            }
                        })
                .start();

        new Thread(
                        () -> {
                            try {
                                ProtocolResponse response =
                                        protocol.getProtocolOutput(url, new Metadata());
                                startTimeSecond.setObject(
                                        Instant.parse(
                                                response.getMetadata()
                                                        .getFirstValue(
                                                                SeleniumProtocol.MD_KEY_START)));
                            } catch (Exception e) {
                                noException.setValue(false);
                            }
                        })
                .start();

        await().until(
                        () ->
                                endTimeFirst.getObject() != null
                                        && startTimeSecond.getObject() != null);

        Instant etf = (Instant) endTimeFirst.getObject();
        Instant sts = (Instant) startTimeSecond.getObject();

        // check that the second call started AFTER the first one finished
        Assert.assertTrue(etf.isBefore(sts));

        Assert.assertTrue(noException.booleanValue());
        protocol.cleanup();
    }

    public static class WildcardResourceHandler extends AbstractHandler {

        @Override
        public void handle(
                String s,
                Request baseRequest,
                jakarta.servlet.http.HttpServletRequest httpServletRequest,
                jakarta.servlet.http.HttpServletResponse response)
                throws IOException {
            if (response.isCommitted() || baseRequest.isHandled()) return;

            baseRequest.setHandled(true);

            final String content = "Success!";
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType("text/html");
            response.setContentLength(content.length());
            try (OutputStream out = response.getOutputStream()) {
                out.write(content.getBytes(StandardCharsets.UTF_8));
            }
        }
    }
}
