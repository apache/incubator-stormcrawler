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

package com.digitalpebble.storm.crawler.protocol.httpclient;

import java.io.IOException;
import java.util.Locale;

import org.apache.commons.lang.StringUtils;
import org.apache.http.Header;
import org.apache.http.HeaderIterator;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.DefaultProxyRoutePlanner;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;

import com.digitalpebble.storm.crawler.Metadata;
import com.digitalpebble.storm.crawler.protocol.AbstractHttpProtocol;
import com.digitalpebble.storm.crawler.protocol.HttpRobotRulesParser;
import com.digitalpebble.storm.crawler.protocol.ProtocolResponse;
import com.digitalpebble.storm.crawler.protocol.RobotRulesParser;
import com.digitalpebble.storm.crawler.util.ConfUtils;

import crawlercommons.robots.BaseRobotRules;

/**
 * Uses Apache httpclient to handle http and https
 **/

public class HttpProtocol extends AbstractHttpProtocol implements
        ResponseHandler<ProtocolResponse> {

    private static final org.slf4j.Logger LOG = LoggerFactory
            .getLogger(HttpProtocol.class);

    private final static PoolingHttpClientConnectionManager CONNECTION_MANAGER = new PoolingHttpClientConnectionManager();
    static {
        // Increase max total connection to 200
        CONNECTION_MANAGER.setMaxTotal(200);
        // Increase default max connection per route to 20
        CONNECTION_MANAGER.setDefaultMaxPerRoute(20);
    }

    private com.digitalpebble.storm.crawler.protocol.HttpRobotRulesParser robots;

    /**
     * TODO record response time in the meta data, see property
     * http.store.responsetime.
     */
    private boolean responseTime = true;

    // TODO find way of limiting the content fetched
    private int maxContent;

    private boolean skipRobots = false;

    private HttpClientBuilder builder;

    private RequestConfig requestConfig;

    @Override
    public void configure(final Config conf) {
        this.maxContent = ConfUtils.getInt(conf, "http.content.limit",
                64 * 1024);
        String userAgent = getAgentString(
                ConfUtils.getString(conf, "http.agent.name"),
                ConfUtils.getString(conf, "http.agent.version"),
                ConfUtils.getString(conf, "http.agent.description"),
                ConfUtils.getString(conf, "http.agent.url"),
                ConfUtils.getString(conf, "http.agent.email"));

        this.responseTime = ConfUtils.getBoolean(conf,
                "http.store.responsetime", true);

        this.skipRobots = ConfUtils.getBoolean(conf, "http.skip.robots", false);

        robots = new HttpRobotRulesParser(conf);

        builder = HttpClients.custom().setUserAgent(userAgent)
                .setConnectionManager(CONNECTION_MANAGER)
                .setConnectionManagerShared(true).disableRedirectHandling();

        String proxyHost = ConfUtils.getString(conf, "http.proxy.host", null);
        int proxyPort = ConfUtils.getInt(conf, "http.proxy.port", 8080);

        boolean useProxy = (proxyHost != null && proxyHost.length() > 0);

        // use a proxy?
        if (useProxy) {
            HttpHost proxy = new HttpHost(proxyHost, proxyPort);
            DefaultProxyRoutePlanner routePlanner = new DefaultProxyRoutePlanner(
                    proxy);
            builder.setRoutePlanner(routePlanner);
        }

        int timeout = ConfUtils.getInt(conf, "http.timeout", 10000);
        requestConfig = RequestConfig.custom().setSocketTimeout(timeout)
                .setConnectTimeout(timeout).build();
    }

    @Override
    public ProtocolResponse getProtocolOutput(String url, Metadata md)
            throws Exception {

        LOG.debug("HTTP connection manager stats {}",
                CONNECTION_MANAGER.getTotalStats());

        HttpGet httpget = new HttpGet(url);
        httpget.setConfig(requestConfig);

        if (md != null) {
            String ifModifiedSince = md.getFirstValue("cachedLastModified");
            if (StringUtils.isNotBlank(ifModifiedSince)) {
                httpget.addHeader("If-Modified-Since", ifModifiedSince);
            }

            String ifNoneMatch = md.getFirstValue("cachedEtag");
            if (StringUtils.isNotBlank(ifNoneMatch)) {
                httpget.addHeader("If-None-Match", ifNoneMatch);
            }
        }

        // no need to release the connection explicitly as this is handled
        // automatically. The client itself must be closed though.
        try (CloseableHttpClient httpclient = builder.build()) {
            return httpclient.execute(httpget, this);
        }
    }

    @Override
    public ProtocolResponse handleResponse(HttpResponse response)
            throws ClientProtocolException, IOException {
        int status = response.getStatusLine().getStatusCode();
        Metadata metadata = new Metadata();
        HeaderIterator iter = response.headerIterator();
        while (iter.hasNext()) {
            Header header = iter.nextHeader();
            metadata.addValue(header.getName().toLowerCase(Locale.ROOT),
                    header.getValue());
        }
        // TODO find a way of limiting by maxContent
        byte[] bytes = EntityUtils.toByteArray(response.getEntity());
        return new ProtocolResponse(bytes, status, metadata);
    }

    @Override
    public BaseRobotRules getRobotRules(String url) {
        if (this.skipRobots)
            return RobotRulesParser.EMPTY_RULES;
        return robots.getRobotRulesSet(this, url);
    }

    public static void main(String args[]) throws Exception {
        HttpProtocol protocol = new HttpProtocol();

        String url = args[0];
        Config conf = ConfUtils.loadConf(args[1]);

        protocol.configure(conf);

        if (!protocol.skipRobots) {
            BaseRobotRules rules = protocol.getRobotRules(url);
            System.out.println("is allowed : " + rules.isAllowed(url));
        }

        Metadata md = new Metadata();
        ProtocolResponse response = protocol.getProtocolOutput(url, md);
        System.out.println(url);
        System.out.println(response.getMetadata());
        System.out.println(response.getStatusCode());
        System.out.println(response.getContent().length);
    }

}
