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

package org.apache.stormcrawler.protocol.okhttp;

import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.common.ConsoleNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import java.io.IOException;
import org.apache.storm.Config;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.protocol.AbstractProtocolTest;
import org.apache.stormcrawler.protocol.IPFilterRules;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Verifies the shipped IP filter default is actually enforced by the HTTP protocol, beyond the
 * standalone rule predicates: a hostname that resolves into a forbidden range must not be connected
 * to, and the documented opt-out must let it through again.
 */
class HttpProtocolIPFilterTest extends AbstractProtocolTest {

    /** The exclude list shipped in crawler-default.yaml. */
    private static final String SHIPPED_EXCLUDE =
            "localhost,sitelocal,linklocal,anylocal,multicast,100.64.0.0/10,0.0.0.0/8,fc00::/7,::/128";

    private Config config(String exclude) {
        final Config conf = new Config();
        conf.put("http.agent.name", "test");
        conf.put("http.agent.version", "1.0");
        conf.put("http.agent.description", "test");
        conf.put("http.agent.url", "http://test.example.com");
        conf.put("http.agent.email", "test@example.com");
        conf.put(IPFilterRules.EXCLUDE_RULES_KEY, exclude);
        return conf;
    }

    private HttpProtocol protocol(Config conf) {
        final HttpProtocol protocol = new HttpProtocol();
        protocol.configure(conf);
        return protocol;
    }

    private HttpProtocol protocol(String exclude) {
        return protocol(config(exclude));
    }

    /**
     * Behind a proxy the socket is connected to the proxy, which resolves the target itself, so the
     * filter would only judge the proxy's address. A proxy on loopback must not block every fetch
     * under the shipped default.
     */
    @Test
    void fetchThroughProxyOnLoopbackIsNotFiltered() throws Exception {
        final WireMockServer proxy =
                new WireMockServer(
                        WireMockConfiguration.options()
                                .dynamicPort()
                                .enableBrowserProxying(true)
                                .notifier(new ConsoleNotifier(false)));
        proxy.start();
        try {
            proxy.stubFor(any(anyUrl()).willReturn(ok("proxied")));
            final Config conf = config(SHIPPED_EXCLUDE);
            conf.put("http.proxy.host", "localhost");
            conf.put("http.proxy.port", proxy.port());
            // the proxy is asked for the target, which is never resolved here
            final String url = "http://target.invalid/page";
            Assertions.assertEquals(
                    200,
                    protocol(conf).getProtocolOutput(url, new Metadata()).getStatusCode(),
                    "a proxy on a private address must not fail the fetch");
            proxy.verify(1, getRequestedFor(urlEqualTo("/page")));
        } finally {
            proxy.stop();
        }
    }

    /** localhost resolves to loopback, which the shipped exclude list blocks. */
    @Test
    void hostnameResolvingToLoopbackIsBlocked() {
        HttpProtocol protocol = protocol("localhost,sitelocal,linklocal,100.64.0.0/10,::/128");
        String url = "http://localhost:" + HTTP_PORT + "/";
        IOException e =
                Assertions.assertThrows(
                        IOException.class,
                        () -> protocol.getProtocolOutput(url, new Metadata()),
                        "a hostname resolving to loopback must not be connected to");
        Assertions.assertTrue(
                e.getMessage() != null && e.getMessage().contains("Forbidden"),
                "the failure must come from the IP filter, not from a refused connection");
    }

    /** The documented opt-out: an empty exclude list lets the same fetch through. */
    @Test
    void optOutAllowsTheSameFetch() throws Exception {
        HttpProtocol protocol = protocol("");
        String url = "http://localhost:" + HTTP_PORT + "/";
        Assertions.assertEquals(
                200,
                protocol.getProtocolOutput(url, new Metadata()).getStatusCode(),
                "with the filter disabled the loopback fetch succeeds");
    }
}
