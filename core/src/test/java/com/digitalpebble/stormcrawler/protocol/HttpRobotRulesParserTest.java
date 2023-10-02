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

    private static final int[] ports = {8089, 8090, 8091, 8092, 8093, 8094, 8095, 8096, 8097};
    private Config conf = new Config();
    private Protocol protocol;
    private String body;
    private String url0 = "http://localhost:" + ports[0];
    private String url1 = "http://localhost:" + ports[1];
    private String url7 = "http://localhost:" + ports[7];
    private String url8 = "http://localhost:" + ports[8];

    @Rule public WireMockRule wireMockRule0 = new WireMockRule(ports[0]);
    @Rule public WireMockRule wireMockRule1 = new WireMockRule(ports[1]);
    @Rule public WireMockRule wireMockRule2 = new WireMockRule(ports[2]);
    @Rule public WireMockRule wireMockRule3 = new WireMockRule(ports[3]);
    @Rule public WireMockRule wireMockRule4 = new WireMockRule(ports[4]);
    @Rule public WireMockRule wireMockRule5 = new WireMockRule(ports[5]);
    @Rule public WireMockRule wireMockRule6 = new WireMockRule(ports[6]);
    @Rule public WireMockRule wireMockRule7 = new WireMockRule(ports[7]);
    @Rule public WireMockRule wireMockRule8 = new WireMockRule(ports[8]);

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

    private void parseRobotRules(int statusCode, Config conf) {
        configureFor(wireMockRule0.port());
        stubFor(
                get(urlPathEqualTo("/robots.txt"))
                        .willReturn(aResponse().withBody(body).withStatus(statusCode)));

        HttpRobotRulesParser httpRobotRulesParser = new HttpRobotRulesParser();
        httpRobotRulesParser.setConf(conf);
        BaseRobotRules robotRules = httpRobotRulesParser.getRobotRulesSet(protocol, url0);

        Assert.assertFalse(robotRules.isAllowAll());
        Assert.assertFalse(robotRules.isAllowNone());
        Assert.assertTrue(robotRules.isAllowed(url0 + "/index.html"));
        Assert.assertFalse(robotRules.isAllowed(url0 + "/restricted/index.html"));
    }

    private void allowAll(int statusCode, Config conf) {
        configureFor(wireMockRule0.port());
        stubFor(
                get(urlPathEqualTo("/robots.txt"))
                        .willReturn(aResponse().withBody(body).withStatus(statusCode)));

        HttpRobotRulesParser httpRobotRulesParser = new HttpRobotRulesParser();
        httpRobotRulesParser.setConf(conf);
        BaseRobotRules robotRules = httpRobotRulesParser.getRobotRulesSet(protocol, url0);

        Assert.assertTrue(robotRules.isAllowAll());
    }

    private void allowNone(int statusCode, Config conf) {
        configureFor(wireMockRule0.port());
        stubFor(
                get(urlPathEqualTo("/robots.txt"))
                        .willReturn(aResponse().withBody(body).withStatus(statusCode)));

        HttpRobotRulesParser httpRobotRulesParser = new HttpRobotRulesParser();
        httpRobotRulesParser.setConf(conf);
        BaseRobotRules robotRules = httpRobotRulesParser.getRobotRulesSet(protocol, url0);

        Assert.assertTrue(robotRules.isAllowNone());
    }

    @Test
    public void testRobotRulesParsing() {
        // Parse as usual for status code 200
        parseRobotRules(200, conf);

        // Allow all for range 300-499 (except 429)
        allowAll(300, conf);
        allowAll(399, conf);
        allowAll(400, conf);
        allowAll(403, conf);
        allowAll(499, conf);

        // Allow none for 429 and range 500-599
        allowNone(429, conf);
        allowNone(500, conf);
        allowNone(599, conf);

        // Allow all for other status codes
        allowAll(299, conf);
        allowAll(777, conf);

        // Special cases for 403 and 5xx
        Config modifiedConf = new Config();
        modifiedConf.putAll(conf);
        modifiedConf.put("http.robots.403.allow", false);
        modifiedConf.put("http.robots.5xx.allow", true);
        allowNone(403, modifiedConf);
        allowAll(500, modifiedConf);
    }

    @Test
    public void testRedirects() {
        // Test for 5 consecutive redirects
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
                        .willReturn(
                                aResponse()
                                        .withHeader(
                                                "location",
                                                "http://localhost:" + ports[6] + "/robots.txt")
                                        .withBody(body)
                                        .withStatus(301)));

        configureFor(wireMockRule6.port());
        stubFor(
                get(urlPathEqualTo("/robots.txt"))
                        .willReturn(aResponse().withBody(body).withStatus(200)));

        HttpRobotRulesParser httpRobotRulesParser = new HttpRobotRulesParser();
        httpRobotRulesParser.setConf(conf);
        BaseRobotRules robotRules = httpRobotRulesParser.getRobotRulesSet(protocol, url1);

        Assert.assertFalse(robotRules.isAllowAll());
        Assert.assertFalse(robotRules.isAllowNone());
        Assert.assertTrue(robotRules.isAllowed(url1 + "/index.html"));
        Assert.assertFalse(robotRules.isAllowed(url1 + "/restricted/index.html"));

        // Test for infinite loop of redirects
        configureFor(wireMockRule1.port());
        stubFor(
                get(urlPathEqualTo("/robots.txt"))
                        .willReturn(
                                aResponse()
                                        .withHeader("location", url1 + "/robots.txt")
                                        .withBody(body)
                                        .withStatus(301)));

        httpRobotRulesParser = new HttpRobotRulesParser();
        httpRobotRulesParser.setConf(conf);
        robotRules = httpRobotRulesParser.getRobotRulesSet(protocol, url1);

        Assert.assertTrue(robotRules.isAllowAll());

        // Test relative redirects
        configureFor(wireMockRule7.port());
        stubFor(
                get(urlPathEqualTo("/robots.txt"))
                        .willReturn(
                                aResponse()
                                        .withHeader(
                                                "location",
                                                "http://localhost:" + ports[8] + "/robots.txt")
                                        .withBody(body)
                                        .withStatus(301)));
        configureFor(wireMockRule8.port());
        stubFor(
                get(urlPathEqualTo("/robots.txt"))
                        .willReturn(
                                aResponse()
                                        .withHeader("location", "http-redirect-robots.txt")
                                        .withBody(body)
                                        .withStatus(302)));
        configureFor(wireMockRule8.port());
        stubFor(
                get(urlPathEqualTo("/http-redirect-robots.txt"))
                        .willReturn(
                                aResponse()
                                        .withHeader(
                                                "location",
                                                "http://localhost:" + ports[5] + "/robots.txt")
                                        .withBody(body)
                                        .withStatus(302)));
        // from here the redirect leads to the same robots.txt as in the first test block

        httpRobotRulesParser = new HttpRobotRulesParser();
        httpRobotRulesParser.setConf(conf);
        robotRules = httpRobotRulesParser.getRobotRulesSet(protocol, url7);

        Assert.assertFalse(robotRules.isAllowAll());
        Assert.assertFalse(robotRules.isAllowNone());
        Assert.assertTrue(robotRules.isAllowed(url1 + "/index.html"));
        Assert.assertFalse(robotRules.isAllowed(url1 + "/restricted/index.html"));

        // repeat without creating a new instance of HttpRobotRulesParser:
        // result should be the same, now with cached rules, even for other locations
        // on the redirect chain where the robots.txt is at the root (URL path: /robots.txt)
        robotRules = httpRobotRulesParser.getRobotRulesSet(protocol, url7);
        Assert.assertFalse(robotRules.isAllowAll());
        Assert.assertFalse(robotRules.isAllowNone());
        Assert.assertTrue(robotRules.isAllowed(url1 + "/index.html"));
        Assert.assertFalse(robotRules.isAllowed(url1 + "/restricted/index.html"));
        robotRules = httpRobotRulesParser.getRobotRulesSet(protocol, url8);
        Assert.assertFalse(robotRules.isAllowAll());
        Assert.assertFalse(robotRules.isAllowNone());
        Assert.assertTrue(robotRules.isAllowed(url1 + "/index.html"));
        Assert.assertFalse(robotRules.isAllowed(url1 + "/restricted/index.html"));
    }
}
