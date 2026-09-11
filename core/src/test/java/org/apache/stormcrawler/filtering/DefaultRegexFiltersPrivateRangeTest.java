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

package org.apache.stormcrawler.filtering;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.filtering.regex.RegexURLFilter;
import org.apache.stormcrawler.protocol.IPFilterRules;
import org.apache.stormcrawler.util.ConfUtils;
import org.apache.stormcrawler.util.URLUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The private-range rules shipped in the archetype default-regex-filters.txt, applied to hosts the
 * JVM resolver maps into loopback, link-local and other non-routable space. The rules judge the
 * bytes of the URL only; the shipped IP filter default is the authoritative check and is verified
 * here as well.
 */
class DefaultRegexFiltersPrivateRangeTest {

    private static final List<String> ARCHETYPE_RULES = new ArrayList<>();

    @BeforeAll
    static void loadShippedRules() throws IOException {
        try (InputStream in =
                DefaultRegexFiltersPrivateRangeTest.class.getResourceAsStream(
                        "/default-regex-filters-archetype.txt")) {
            Assertions.assertNotNull(in, "the archetype rules file must be on the test classpath");
            for (String line :
                    new String(in.readAllBytes(), StandardCharsets.UTF_8).split("\\r?\\n")) {
                String trimmed = line.trim();
                if (!trimmed.isEmpty() && !trimmed.startsWith("#")) {
                    ARCHETYPE_RULES.add(trimmed);
                }
            }
        }
    }

    private URLFilter createFilter() {
        ObjectNode filterParams = new ObjectNode(JsonNodeFactory.instance);
        ArrayNode rules = filterParams.putArray("urlFilters");
        for (String rule : ARCHETYPE_RULES) {
            rules.add(rule);
        }
        RegexURLFilter filter = new RegexURLFilter();
        Map<String, Object> conf = new HashMap<>();
        filter.configure(conf, filterParams);
        return filter;
    }

    private void assertRejected(URLFilter filter, String url) throws MalformedURLException {
        URL source = URLUtil.toURL("http://www.example.com/index.html");
        Assertions.assertNull(filter.filter(source, new Metadata(), url), url);
    }

    private void assertAccepted(URLFilter filter, String url) throws MalformedURLException {
        URL source = URLUtil.toURL("http://www.example.com/index.html");
        Assertions.assertNotNull(filter.filter(source, new Metadata(), url), url);
    }

    @Test
    void dottedQuadFormsAreRejected() throws MalformedURLException {
        URLFilter filter = createFilter();
        assertRejected(filter, "http://127.0.0.1/");
        assertRejected(filter, "http://10.0.0.5/");
        assertRejected(filter, "http://192.168.1.1/");
        assertRejected(filter, "http://172.16.0.1/");
    }

    @Test
    void otherNonRoutableRangesAreRejected() throws MalformedURLException {
        URLFilter filter = createFilter();
        assertRejected(filter, "http://169.254.169.254/");
        assertRejected(filter, "http://100.64.0.1/");
        assertRejected(filter, "http://100.65.0.1/");
        assertRejected(filter, "http://100.127.255.254/");
        assertRejected(filter, "http://0.0.0.0/");
        assertRejected(filter, "http://[fd00::1]/");
        assertRejected(filter, "http://[fe80::1]/");
    }

    /** The CGNAT range starts at 100.64; the public space right below stays reachable. */
    @Test
    void publicAddressesRightBelowCgnatAreAccepted() throws MalformedURLException {
        URLFilter filter = createFilter();
        assertAccepted(filter, "http://100.63.0.1/");
        assertAccepted(filter, "http://100.128.0.1/");
    }

    /** The rules are case-insensitive: upper-case spellings are caught too. */
    @Test
    void caseInsensitiveSpellingsAreRejected() throws MalformedURLException {
        URLFilter filter = createFilter();
        assertRejected(filter, "http://LOCALHOST:8080/");
        assertRejected(filter, "http://[FD00::1]/");
        assertRejected(filter, "http://[FE80::1]/");
    }

    /** Loopback reached through an IPv4-mapped or fully expanded IPv6 literal. */
    @Test
    void ipv6LoopbackSpellingsAreRejected() throws MalformedURLException {
        URLFilter filter = createFilter();
        assertRejected(filter, "http://[::ffff:127.0.0.1]/");
        assertRejected(filter, "http://[0:0:0:0:0:0:0:1]/");
    }

    /** Both host forms are resolved to 127.0.0.1 by InetAddress.getByName. */
    @Test
    void abbreviatedAndIntegerLoopbackFormsAreRejected() throws MalformedURLException {
        URLFilter filter = createFilter();
        assertRejected(filter, "http://127.1/");
        assertRejected(filter, "http://2130706433/");
    }

    /**
     * The resolver rejects hex (0x7f000001) and does not read leading zeros as octal (0177.0.0.1 is
     * 177.0.0.1, public), so the \d{1,10} rule closes the only numeric form that actually works.
     */
    @Test
    void resolverTreatmentOfNumericForms() throws Exception {
        Assertions.assertThrows(
                java.net.UnknownHostException.class, () -> InetAddress.getByName("0x7f000001"));
        Assertions.assertEquals("177.0.0.1", InetAddress.getByName("0177.0.0.1").getHostAddress());
    }

    /** The fetcher must not reach private space at all: the shipped IP filter default. */
    @Test
    void ipFilterExcludeListIsShippedEnabled() throws Exception {
        Map<String, Object> defaults =
                org.apache.storm.utils.Utils.findAndReadConfigFile("crawler-default.yaml", false);
        Map<String, Object> conf = ConfUtils.extractConfigElement(defaults);
        String exclude = ConfUtils.getString(conf, "http.filter.ipaddress.exclude", null);
        Assertions.assertNotNull(
                exclude, "http.filter.ipaddress.exclude must be enabled in crawler-default.yaml");
        IPFilterRules ipFilter = new IPFilterRules(conf);
        Assertions.assertFalse(ipFilter.isEmpty());

        // every excluded range, including its boundaries
        Assertions.assertFalse(ipFilter.accept(InetAddress.getByName("127.0.0.1")), "loopback");
        Assertions.assertFalse(ipFilter.accept(InetAddress.getByName("::1")), "IPv6 loopback");
        Assertions.assertFalse(ipFilter.accept(InetAddress.getByName("10.0.0.0")), "RFC1918 /8");
        Assertions.assertFalse(ipFilter.accept(InetAddress.getByName("10.255.255.255")));
        Assertions.assertFalse(ipFilter.accept(InetAddress.getByName("172.16.0.0")), "RFC1918 /12");
        Assertions.assertFalse(ipFilter.accept(InetAddress.getByName("172.31.255.255")));
        Assertions.assertFalse(
                ipFilter.accept(InetAddress.getByName("192.168.0.0")), "RFC1918 /16");
        Assertions.assertFalse(
                ipFilter.accept(InetAddress.getByName("169.254.169.254")), "linklocal");
        Assertions.assertFalse(ipFilter.accept(InetAddress.getByName("fe80::1")), "IPv6 linklocal");
        Assertions.assertFalse(ipFilter.accept(InetAddress.getByName("100.64.0.0")), "CGNAT");
        Assertions.assertFalse(ipFilter.accept(InetAddress.getByName("100.127.255.255")));
        Assertions.assertFalse(ipFilter.accept(InetAddress.getByName("0.0.0.0")), "this network");
        Assertions.assertFalse(ipFilter.accept(InetAddress.getByName("0.1.2.3")), "0/8");
        Assertions.assertFalse(ipFilter.accept(InetAddress.getByName("fc00::1")), "ULA low");
        Assertions.assertFalse(ipFilter.accept(InetAddress.getByName("fd00::1")), "ULA high");
        Assertions.assertFalse(ipFilter.accept(InetAddress.getByName("::")), "IPv6 unspecified");

        // public boundary addresses just outside the excluded ranges are allowed
        Assertions.assertTrue(ipFilter.accept(InetAddress.getByName("9.255.255.255")));
        Assertions.assertTrue(ipFilter.accept(InetAddress.getByName("11.0.0.0")));
        Assertions.assertTrue(ipFilter.accept(InetAddress.getByName("172.15.255.255")));
        Assertions.assertTrue(ipFilter.accept(InetAddress.getByName("172.32.0.0")));
        Assertions.assertTrue(ipFilter.accept(InetAddress.getByName("192.167.255.255")));
        Assertions.assertTrue(ipFilter.accept(InetAddress.getByName("192.169.0.0")));
        Assertions.assertTrue(ipFilter.accept(InetAddress.getByName("100.63.255.255")));
        Assertions.assertTrue(ipFilter.accept(InetAddress.getByName("100.128.0.0")));
        Assertions.assertTrue(ipFilter.accept(InetAddress.getByName("1.1.1.1")));
        Assertions.assertTrue(ipFilter.accept(InetAddress.getByName("140.211.11.131")));
        Assertions.assertTrue(ipFilter.accept(InetAddress.getByName("2001:4860::1")));
    }

    /** The documented opt-out: an empty exclude list accepts everything again. */
    @Test
    void explicitOptOutDisablesTheFilter() throws Exception {
        Map<String, Object> conf = new HashMap<>();
        conf.put(IPFilterRules.EXCLUDE_RULES_KEY, "");
        IPFilterRules ipFilter = new IPFilterRules(conf);
        Assertions.assertTrue(ipFilter.isEmpty(), "empty exclude must install no rules");
        Assertions.assertTrue(ipFilter.accept(InetAddress.getLoopbackAddress()));
        Assertions.assertTrue(ipFilter.accept(InetAddress.getByName("169.254.169.254")));
    }
}
