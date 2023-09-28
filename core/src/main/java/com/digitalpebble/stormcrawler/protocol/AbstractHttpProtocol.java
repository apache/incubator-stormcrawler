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

import com.digitalpebble.stormcrawler.proxy.ProxyManager;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import com.digitalpebble.stormcrawler.util.InitialisationUtil;
import crawlercommons.robots.BaseRobotRules;
import java.util.LinkedList;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.Config;
import org.slf4j.LoggerFactory;

public abstract class AbstractHttpProtocol implements Protocol {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(AbstractHttpProtocol.class);

    private com.digitalpebble.stormcrawler.protocol.HttpRobotRulesParser robots;

    protected boolean skipRobots = false;

    protected boolean storeHTTPHeaders = false;

    protected boolean useCookies = false;

    protected List<String> protocolVersions;

    protected static final String RESPONSE_COOKIES_HEADER = "set-cookie";

    protected static final String SET_HEADER_BY_REQUEST = "set-header";

    protected String protocolMDprefix = "";

    public ProxyManager proxyManager;

    protected final List<KeyValue> customHeaders = new LinkedList<>();

    protected static class KeyValue {
        private final String k;
        private final String v;

        public String getKey() {
            return k;
        }

        public String getValue() {
            return v;
        }

        public KeyValue(String k, String v) {
            super();
            this.k = k;
            this.v = v;
        }

        public static KeyValue build(String h) {
            int pos = h.indexOf("=");
            if (pos == -1) return new KeyValue(h.trim(), "");
            if (pos + 1 == h.length()) return new KeyValue(h.trim(), "");
            return new KeyValue(h.substring(0, pos).trim(), h.substring(pos + 1).trim());
        }
    }

    @Override
    public void configure(Config conf) {
        this.skipRobots = ConfUtils.getBoolean(conf, "http.robots.file.skip", false);

        this.storeHTTPHeaders = ConfUtils.getBoolean(conf, "http.store.headers", false);
        this.useCookies = ConfUtils.getBoolean(conf, "http.use.cookies", false);
        this.protocolVersions = ConfUtils.loadListFromConf("http.protocol.versions", conf);

        List<String> headers = ConfUtils.loadListFromConf("http.custom.headers", conf);
        for (String h : headers) {
            customHeaders.add(KeyValue.build(h));
        }

        robots = new HttpRobotRulesParser(conf);
        protocolMDprefix =
                ConfUtils.getString(
                        conf, ProtocolResponse.PROTOCOL_MD_PREFIX_PARAM, protocolMDprefix);

        String proxyManagerImplementation =
                ConfUtils.getString(
                        conf,
                        "http.proxy.manager",
                        // determine whether to set default as
                        // SingleProxyManager by
                        // checking whether legacy proxy field is set
                        (ConfUtils.getString(conf, "http.proxy.host", null) != null)
                                ? "com.digitalpebble.stormcrawler.proxy.SingleProxyManager"
                                : null);

        // conditionally load proxy manager
        if (proxyManagerImplementation != null) {
            try {
                // create new proxy manager from file
                proxyManager =
                        InitialisationUtil.initializeFromQualifiedName(
                                proxyManagerImplementation, ProxyManager.class);
                proxyManager.configure(conf);
            } catch (Exception e) {
                LOG.error("Failed to create proxy manager `" + proxyManagerImplementation + "`", e);
            }
        }
    }

    @Override
    public BaseRobotRules getRobotRules(String url) {
        if (this.skipRobots) return RobotRulesParser.EMPTY_RULES;
        return robots.getRobotRulesSet(this, url);
    }

    @Override
    public void cleanup() {}

    public static String getAgentString(Config conf) {
        String agent = ConfUtils.getString(conf, "http.agent");
        if (agent != null && !agent.isEmpty()) {
            return agent;
        }
        return getAgentString(
                ConfUtils.getString(conf, "http.agent.name"),
                ConfUtils.getString(conf, "http.agent.version"),
                ConfUtils.getString(conf, "http.agent.description"),
                ConfUtils.getString(conf, "http.agent.url"),
                ConfUtils.getString(conf, "http.agent.email"));
    }

    private static String getAgentString(
            String agentName,
            String agentVersion,
            String agentDesc,
            String agentURL,
            String agentEmail) {

        StringBuilder buf = new StringBuilder();

        buf.append(agentName);

        if (StringUtils.isNotBlank(agentVersion)) {
            buf.append("/");
            buf.append(agentVersion);
        }

        boolean hasAgentDesc = StringUtils.isNotBlank(agentDesc);
        boolean hasAgentURL = StringUtils.isNotBlank(agentURL);
        boolean hasAgentEmail = StringUtils.isNotBlank(agentEmail);

        if (hasAgentDesc || hasAgentEmail || hasAgentURL) {
            buf.append(" (");

            if (hasAgentDesc) {
                buf.append(agentDesc);
                if (hasAgentURL || hasAgentEmail) buf.append("; ");
            }

            if (hasAgentURL) {
                buf.append(agentURL);
                if (hasAgentEmail) buf.append("; ");
            }

            if (hasAgentEmail) {
                buf.append(agentEmail);
            }

            buf.append(")");
        }

        return buf.toString();
    }
}
