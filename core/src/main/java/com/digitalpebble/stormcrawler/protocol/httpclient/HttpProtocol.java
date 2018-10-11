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

package com.digitalpebble.stormcrawler.protocol.httpclient;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.http.Header;
import org.apache.http.HeaderIterator;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.cookie.Cookie;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.DefaultProxyRoutePlanner;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.Args;
import org.apache.http.util.ByteArrayBuffer;
import org.apache.storm.Config;
import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.persistence.Status;
import com.digitalpebble.stormcrawler.protocol.AbstractHttpProtocol;
import com.digitalpebble.stormcrawler.protocol.ProtocolResponse;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import com.digitalpebble.stormcrawler.util.CookieConverter;

/**
 * Uses Apache httpclient to handle http and https
 **/

public class HttpProtocol extends AbstractHttpProtocol implements
        ResponseHandler<ProtocolResponse> {

    private static final org.slf4j.Logger LOG = LoggerFactory
            .getLogger(HttpProtocol.class);

    private static final PoolingHttpClientConnectionManager CONNECTION_MANAGER = new PoolingHttpClientConnectionManager();

    private int maxContent;

    private HttpClientBuilder builder;

    private RequestConfig requestConfig;

    @Override
    public void configure(final Config conf) {

        super.configure(conf);

        // allow up to 200 connections or same as the number of threads used for
        // fetching
        int maxFetchThreads = ConfUtils.getInt(conf, "fetcher.threads.number",
                200);
        CONNECTION_MANAGER.setMaxTotal(maxFetchThreads);
        int maxPerRoute = ConfUtils
                .getInt(conf, "fetcher.threads.per.queue", 1);
        if (maxPerRoute < 20) {
            maxPerRoute = 20;
        }
        CONNECTION_MANAGER.setDefaultMaxPerRoute(maxPerRoute);

        this.maxContent = ConfUtils.getInt(conf, "http.content.limit", -1);

        String userAgent = getAgentString(
                ConfUtils.getString(conf, "http.agent.name"),
                ConfUtils.getString(conf, "http.agent.version"),
                ConfUtils.getString(conf, "http.agent.description"),
                ConfUtils.getString(conf, "http.agent.url"),
                ConfUtils.getString(conf, "http.agent.email"));

        Collection<BasicHeader> defaultHeaders = new LinkedList<>();

        String accept = ConfUtils.getString(conf, "http.accept");
        if (StringUtils.isNotBlank(accept)) {
            defaultHeaders.add(new BasicHeader("Accept", accept));
        }

        String basicAuthUser = ConfUtils.getString(conf, "http.basicauth.user",
                null);

        // use a basic auth?
        if (StringUtils.isNotBlank(basicAuthUser)) {
            String basicAuthPass = ConfUtils.getString(conf,
                    "http.basicauth.password", "");
            String encoding = Base64.getEncoder().encodeToString(
                    (basicAuthUser + ":" + basicAuthPass).getBytes());
            defaultHeaders.add(new BasicHeader("Authorization", "Basic "
                    + encoding));
        }

        String acceptLanguage = ConfUtils.getString(conf,
                "http.accept.language");
        if (StringUtils.isNotBlank(acceptLanguage)) {
            defaultHeaders.add(new BasicHeader("Accept-Language",
                    acceptLanguage));
        }

        builder = HttpClients.custom().setUserAgent(userAgent)
                .setDefaultHeaders(defaultHeaders)
                .setConnectionManager(CONNECTION_MANAGER)
                .setConnectionManagerShared(true).disableRedirectHandling()
                .disableAutomaticRetries();

        int timeout = ConfUtils.getInt(conf, "http.timeout", 10000);

        RequestConfig.Builder requestConfigBuilder = RequestConfig.custom()
                .setSocketTimeout(timeout).setConnectTimeout(timeout)
                .setConnectionRequestTimeout(timeout)
                .setCookieSpec(CookieSpecs.STANDARD);

        String proxyHost = ConfUtils.getString(conf, "http.proxy.host", null);
        int proxyPort = ConfUtils.getInt(conf, "http.proxy.port", 8080);

        boolean useProxy = proxyHost != null && proxyHost.length() > 0;

        // use a proxy?
        if (useProxy) {

            String proxyUser = ConfUtils.getString(conf, "http.proxy.user",
                    null);
            String proxyPass = ConfUtils.getString(conf, "http.proxy.pass",
                    null);

            if (StringUtils.isNotBlank(proxyUser)
                    && StringUtils.isNotBlank(proxyPass)) {
                List<String> authSchemes = new ArrayList<>();
                // Can make configurable and add more in future
                authSchemes.add(AuthSchemes.BASIC);
                requestConfigBuilder.setProxyPreferredAuthSchemes(authSchemes);

                BasicCredentialsProvider basicAuthCreds = new BasicCredentialsProvider();
                basicAuthCreds.setCredentials(new AuthScope(proxyHost,
                        proxyPort), new UsernamePasswordCredentials(proxyUser,
                        proxyPass));
                builder.setDefaultCredentialsProvider(basicAuthCreds);
            }

            HttpHost proxy = new HttpHost(proxyHost, proxyPort);
            DefaultProxyRoutePlanner routePlanner = new DefaultProxyRoutePlanner(
                    proxy);
            builder.setRoutePlanner(routePlanner);
        }

        requestConfig = requestConfigBuilder.build();
    }

    @Override
    public ProtocolResponse getProtocolOutput(String url, Metadata md)
            throws Exception {

        LOG.debug("HTTP connection manager stats {}",
                CONNECTION_MANAGER.getTotalStats());

        HttpRequestBase request = new HttpGet(url);

        if (md != null) {
            String useHead = md.getFirstValue("http.method.head");
            if ("true".equalsIgnoreCase(useHead)) {
                request = new HttpHead(url);
            }

            String lastModified = md.getFirstValue("last-modified");
            if (StringUtils.isNotBlank(lastModified)) {
                request.addHeader("If-Modified-Since", lastModified);
            }

            String ifNoneMatch = md.getFirstValue("etag");
            if (StringUtils.isNotBlank(ifNoneMatch)) {
                request.addHeader("If-None-Match", ifNoneMatch);
            }

            String accept = md.getFirstValue("http.accept");
            if (StringUtils.isNotBlank(accept)) {
                request.setHeader(new BasicHeader("Accept", accept));
            }

            String acceptLanguage = md.getFirstValue("http.accept.language");
            if (StringUtils.isNotBlank(acceptLanguage)) {
                request.setHeader(new BasicHeader("Accept-Language",
                        acceptLanguage));
            }

            if (useCookies) {
                addCookiesToRequest(request, md);
            }
        }

        request.setConfig(requestConfig);

        // no need to release the connection explicitly as this is handled
        // automatically. The client itself must be closed though.
        try (CloseableHttpClient httpclient = builder.build()) {
            return httpclient.execute(request, this);
        }
    }

    private void addCookiesToRequest(HttpRequestBase request, Metadata md) {
        String[] cookieStrings = md.getValues(RESPONSE_COOKIES_HEADER);
        if (cookieStrings != null && cookieStrings.length > 0) {
            List<Cookie> cookies;
            try {
                cookies = CookieConverter.getCookies(cookieStrings, request
                        .getURI().toURL());
                for (Cookie c : cookies) {
                    request.addHeader("Cookie",
                            c.getName() + "=" + c.getValue());
                }
            } catch (MalformedURLException e) { // Bad url , nothing to do
            }
        }
    }

    @Override
    public ProtocolResponse handleResponse(HttpResponse response)
            throws IOException {

        StatusLine statusLine = response.getStatusLine();
        int status = statusLine.getStatusCode();

        StringBuilder verbatim = new StringBuilder();
        if (storeHTTPHeaders) {
            verbatim.append(statusLine.toString()).append("\r\n");
        }

        Metadata metadata = new Metadata();
        HeaderIterator iter = response.headerIterator();
        while (iter.hasNext()) {
            Header header = iter.nextHeader();
            if (storeHTTPHeaders) {
                verbatim.append(header.toString()).append("\r\n");
            }
            metadata.addValue(header.getName().toLowerCase(Locale.ROOT),
                    header.getValue());
        }

        MutableBoolean trimmed = new MutableBoolean();

        byte[] bytes = new byte[] {};

        if (!Status.REDIRECTION.equals(Status.fromHTTPCode(status))) {
            bytes = HttpProtocol.toByteArray(response.getEntity(), maxContent,
                    trimmed);
            if (trimmed.booleanValue()) {
                metadata.setValue("http.trimmed", "true");
                LOG.warn("HTTP content trimmed to {}", bytes.length);
            }
        }

        if (storeHTTPHeaders) {
            verbatim.append("\r\n");
            metadata.setValue("_response.headers_", verbatim.toString());
        }

        return new ProtocolResponse(bytes, status, metadata);
    }

    private static final byte[] toByteArray(final HttpEntity entity,
            int maxContent, MutableBoolean trimmed) throws IOException {

        if (entity == null)
            return new byte[] {};

        final InputStream instream = entity.getContent();
        if (instream == null) {
            return null;
        }
        Args.check(entity.getContentLength() <= Integer.MAX_VALUE,
                "HTTP entity too large to be buffered in memory");
        int reportedLength = (int) entity.getContentLength();
        // set default size for buffer: 100 KB
        int bufferInitSize = 102400;
        if (reportedLength != -1) {
            bufferInitSize = reportedLength;
        }
        // avoid init of too large a buffer when we will trim anyway
        if (maxContent != -1 && bufferInitSize > maxContent) {
            bufferInitSize = maxContent;
        }
        final ByteArrayBuffer buffer = new ByteArrayBuffer(bufferInitSize);
        final byte[] tmp = new byte[4096];
        int lengthRead;
        while ((lengthRead = instream.read(tmp)) != -1) {
            // check whether we need to trim
            if (maxContent != -1 && buffer.length() + lengthRead > maxContent) {
                buffer.append(tmp, 0, maxContent - buffer.length());
                trimmed.setValue(true);
                break;
            }
            buffer.append(tmp, 0, lengthRead);
        }
        return buffer.toByteArray();
    }

    public static void main(String args[]) throws Exception {
        HttpProtocol.main(new HttpProtocol(), args);
    }

}