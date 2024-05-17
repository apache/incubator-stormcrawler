/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.stormcrawler.protocol.playwright;

import java.time.Instant;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.storm.Config;
import org.apache.storm.utils.MutableObject;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.protocol.AbstractProtocolTest;
import org.apache.stormcrawler.protocol.ProtocolResponse;
import org.eclipse.jetty.server.Handler;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for Playwright protocol implementation. Chrome should be running on localhost, whether is
 * has been launched manually or by Playwright.
 */
public class ProtocolTest extends AbstractProtocolTest {

    @Rule public Timeout globalTimeout = Timeout.seconds(120);

    private static final Logger LOG = LoggerFactory.getLogger(ProtocolTest.class);

    private static final String USER_AGENT = "StormCrawlerTest";

    @Override
    protected Handler[] getHandlers() {
        return new Handler[] {new LocalResourceHandler(), new WildcardResourceHandler()};
    }

    public HttpProtocol getProtocol() {

        Config conf = new Config();
        conf.put("http.agent.name", USER_AGENT);

        // "http://localhost:9222"
        String cdpurl = System.getProperty("playwright.cdp.url");
        if (cdpurl != null) {
            conf.put("playwright.cdp.url", cdpurl);
        }

        HttpProtocol protocol = new HttpProtocol();
        protocol.configure(conf);
        return protocol;
    }

    @Test
    public void testNotFound() throws Exception {
        HttpProtocol protocol = getProtocol();
        String url = "http://localhost:" + HTTP_PORT + "/doesNotExist";
        ProtocolResponse response = protocol.getProtocolOutput(url, new Metadata());
        Assert.assertEquals(404, response.getStatusCode());
        Assert.assertEquals(0, response.getContent().length);
    }

    @Test
    public void testPDF() throws Exception {
        HttpProtocol protocol = getProtocol();
        String url = "http://localhost:" + HTTP_PORT + "/pdf-test.pdf";
        ProtocolResponse response = protocol.getProtocolOutput(url, new Metadata());
        // check that we have the metadata we expect
        Assert.assertNotNull(response.getMetadata().getFirstValue("key"));
        // the correct code
        Assert.assertEquals(200, response.getStatusCode());
        Assert.assertEquals(32027, response.getContent().length);
    }

    /** Calls 2 URLs on the same protocol instance - should block * */
    @Test
    public void testBlocking() throws InterruptedException {
        HttpProtocol p = getProtocol();
        run(p, p, true);
    }

    /** Calls 2 URLs on 2 different instances - should run in parallel * */
    @Test
    public void testParallel() throws InterruptedException {
        run(getProtocol(), getProtocol(), false);
    }

    private void run(HttpProtocol protocol, HttpProtocol protocol2, boolean expected)
            throws InterruptedException {

        MutableBoolean noException = new MutableBoolean(true);

        MutableObject endTimeFirst = new MutableObject();
        MutableObject startTimeSecond = new MutableObject();

        String url = "http://localhost:" + HTTP_PORT + "/";

        new Thread(
                        () -> {
                            try {
                                ProtocolResponse response =
                                        protocol.getProtocolOutput(url, new Metadata());
                                endTimeFirst.setObject(
                                        Instant.parse(
                                                response.getMetadata()
                                                        .getFirstValue(HttpProtocol.MD_KEY_END)));
                            } catch (Exception e) {
                                noException.setValue(false);
                            }
                        })
                .start();

        new Thread(
                        () -> {
                            try {
                                ProtocolResponse response =
                                        protocol2.getProtocolOutput(url, new Metadata());
                                startTimeSecond.setObject(
                                        Instant.parse(
                                                response.getMetadata()
                                                        .getFirstValue(HttpProtocol.MD_KEY_START)));
                            } catch (Exception e) {
                                noException.setValue(false);
                            }
                        })
                .start();

        while (noException.booleanValue()
                && (endTimeFirst.getObject() == null || startTimeSecond.getObject() == null)) {
            Thread.sleep(10);
        }

        Assert.assertEquals(true, noException.booleanValue());

        Instant etf = (Instant) endTimeFirst.getObject();
        Instant sts = (Instant) startTimeSecond.getObject();

        // check that the second call started while the first one is being processed
        // normal - dealing with different instances
        Assert.assertEquals(expected, etf.isBefore(sts));

        protocol.cleanup();
    }
}
