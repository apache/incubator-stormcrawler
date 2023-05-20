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
package com.digitalpebble.stormcrawler.protocol;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.configureFor;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import crawlercommons.robots.BaseRobotRules;
import org.apache.storm.Config;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class HttpRobotRulesParserTest {

    private static final int[] ports = {8089, 8090, 8091, 8092, 8093, 8094};
    private Config conf = new Config();
    private Protocol protocol;
    private String body;
    private String url = "http://localhost:" + ports[0];

    @Rule public WireMockRule wireMockRule0 = new WireMockRule(ports[0]);
    @Rule public WireMockRule wireMockRule1 = new WireMockRule(ports[1]);
    @Rule public WireMockRule wireMockRule2 = new WireMockRule(ports[2]);
    @Rule public WireMockRule wireMockRule3 = new WireMockRule(ports[3]);
    @Rule public WireMockRule wireMockRule4 = new WireMockRule(ports[4]);
    @Rule public WireMockRule wireMockRule5 = new WireMockRule(ports[5]);

    @Before
    public void setUp() throws Exception {
        conf.put("http.agent.name", "this.is.only.a.test");
        ProtocolFactory protocolFactory = ProtocolFactory.getInstance(conf);
        protocol = protocolFactory.getProtocol("http")[0];
        protocolFactory.cleanup();

        String newLine = System.getProperty("line.separator");
        body =
                new StringBuilder()
                        .append("User-agent: this.is.only.a.test")
                        .append(newLine)
                        .append("Disallow: /restricted/")
                        .toString();
    }

    @Test
    public void testRedirects() {
        // Test for 5 consecutive redirects
        configureFor(wireMockRule0.port());
        stubFor(
                get(urlPathEqualTo("/robots.txt"))
                        .willReturn(
                                aResponse()
                                        .withHeader(
                                                "location",
                                                "http://localhost:" + ports[1] + "/robots.txt")
                                        .withBody(body)
                                        .withStatus(301)));

        configureFor(wireMockRule1.port());
        stubFor(
                get(urlPathEqualTo("/robots.txt"))
                        .willReturn(
                                aResponse()
                                        .withHeader(
                                                "location",
                                                "http://localhost:" + ports[2] + "/robots.txt")
                                        .withBody(body)
                                        .withStatus(301)));

        configureFor(wireMockRule2.port());
        stubFor(
                get(urlPathEqualTo("/robots.txt"))
                        .willReturn(
                                aResponse()
                                        .withHeader(
                                                "location",
                                                "http://localhost:" + ports[3] + "/robots.txt")
                                        .withBody(body)
                                        .withStatus(301)));

        configureFor(wireMockRule3.port());
        stubFor(
                get(urlPathEqualTo("/robots.txt"))
                        .willReturn(
                                aResponse()
                                        .withHeader(
                                                "location",
                                                "http://localhost:" + ports[4] + "/robots.txt")
                                        .withBody(body)
                                        .withStatus(301)));

        configureFor(wireMockRule4.port());
        stubFor(
                get(urlPathEqualTo("/robots.txt"))
                        .willReturn(
                                aResponse()
                                        .withHeader(
                                                "location",
                                                "http://localhost:" + ports[5] + "/robots.txt")
                                        .withBody(body)
                                        .withStatus(301)));

        configureFor(wireMockRule5.port());
        stubFor(
                get(urlPathEqualTo("/robots.txt"))
                        .willReturn(aResponse().withBody(body).withStatus(200)));

        HttpRobotRulesParser httpRobotRulesParser = new HttpRobotRulesParser();
        httpRobotRulesParser.setConf(conf);
        BaseRobotRules robotRules = httpRobotRulesParser.getRobotRulesSet(protocol, url);

        Assert.assertFalse(robotRules.isAllowAll());
        Assert.assertFalse(robotRules.isAllowNone());
        Assert.assertTrue(robotRules.isAllowed("http://localhost:" + ports[0] + "/index.html"));
        Assert.assertFalse(
                robotRules.isAllowed("http://localhost:" + ports[0] + "/restricted/index.html"));

        // Test for infinite loop of redirects
        configureFor(wireMockRule0.port());
        stubFor(
                get(urlPathEqualTo("/robots.txt"))
                        .willReturn(
                                aResponse()
                                        .withHeader(
                                                "location",
                                                "http://localhost:" + ports[0] + "/robots.txt")
                                        .withBody(body)
                                        .withStatus(301)));

        httpRobotRulesParser = new HttpRobotRulesParser();
        httpRobotRulesParser.setConf(conf);
        robotRules = httpRobotRulesParser.getRobotRulesSet(protocol, url);

        Assert.assertTrue(robotRules.isAllowAll());
    }
}
