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

package com.digitalpebble.stormcrawler.protocol.okhttp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.URL;
import java.security.cert.CertificateException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.mutable.MutableObject;
import org.apache.http.cookie.Cookie;
import org.apache.storm.Config;
import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.protocol.AbstractHttpProtocol;
import com.digitalpebble.stormcrawler.protocol.HttpHeaders;
import com.digitalpebble.stormcrawler.protocol.ProtocolResponse;
import com.digitalpebble.stormcrawler.protocol.ProtocolResponse.TrimmedContentReason;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import com.digitalpebble.stormcrawler.util.CookieConverter;

import okhttp3.Call;
import okhttp3.Connection;
import okhttp3.Headers;
import okhttp3.Interceptor;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Request.Builder;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okio.BufferedSource;

public class HttpProtocol extends AbstractHttpProtocol {

    private static final org.slf4j.Logger LOG = LoggerFactory
            .getLogger(HttpProtocol.class);

    private final MediaType JSON = MediaType
            .parse("application/json; charset=utf-8");

    private OkHttpClient client;

    private int maxContent;

    private int completionTimeout = -1;

    /** Accept partially fetched content as trimmed content */
    private boolean partialContentAsTrimmed = false;

    private final static String VERBATIM_REQUEST_KEY = "_request.headers_";
    private final static String VERBATIM_RESPONSE_KEY = "_response.headers_";
    private final static String VERBATIM_RESPONSE_IP_KEY = "_response.ip_";
    private final static String VERBATIM_REQUEST_TIME_KEY = "_request.time_";

    private final List<String[]> customRequestHeaders = new LinkedList<>();

    private static final TrustManager[] trustAllCerts = new TrustManager[] { new X509TrustManager() {
        @Override
        public void checkClientTrusted(
                java.security.cert.X509Certificate[] chain, String authType)
                throws CertificateException {
        }

        @Override
        public void checkServerTrusted(
                java.security.cert.X509Certificate[] chain, String authType)
                throws CertificateException {
        }

        @Override
        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
            return new java.security.cert.X509Certificate[] {};
        }
    } };

    private static final SSLContext trustAllSslContext;
    static {
        try {
            trustAllSslContext = SSLContext.getInstance("SSL");
            trustAllSslContext.init(null, trustAllCerts,
                    new java.security.SecureRandom());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    private static final SSLSocketFactory trustAllSslSocketFactory = trustAllSslContext
            .getSocketFactory();

    @Override
    public void configure(Config conf) {
        super.configure(conf);

        this.maxContent = ConfUtils.getInt(conf, "http.content.limit", -1);

        int timeout = ConfUtils.getInt(conf, "http.timeout", 10000);

        this.completionTimeout = ConfUtils.getInt(conf,
                "topology.message.timeout.secs", completionTimeout);

        this.partialContentAsTrimmed = ConfUtils.getBoolean(conf,
                "http.content.partial.as.trimmed", false);

        okhttp3.OkHttpClient.Builder builder = new OkHttpClient.Builder()
                .retryOnConnectionFailure(true).followRedirects(false)
                .connectTimeout(timeout, TimeUnit.MILLISECONDS)
                .writeTimeout(timeout, TimeUnit.MILLISECONDS)
                .readTimeout(timeout, TimeUnit.MILLISECONDS);

        String userAgent = getAgentString(conf);
        if (StringUtils.isNotBlank(userAgent)) {
            customRequestHeaders.add(new String[] { "User-Agent", userAgent });
        }

        String accept = ConfUtils.getString(conf, "http.accept");
        if (StringUtils.isNotBlank(accept)) {
            customRequestHeaders.add(new String[] { "Accept", accept });
        }

        String acceptLanguage = ConfUtils.getString(conf,
                "http.accept.language");
        if (StringUtils.isNotBlank(acceptLanguage)) {
            customRequestHeaders.add(new String[] { "Accept-Language",
                    acceptLanguage });
        }

        String proxyHost = ConfUtils.getString(conf, "http.proxy.host", null);
        String proxyType = ConfUtils.getString(conf, "http.proxy.type", "HTTP");
        int proxyPort = ConfUtils.getInt(conf, "http.proxy.port", 8080);

        boolean useProxy = proxyHost != null && proxyHost.length() > 0;

        // use a proxy?
        if (useProxy) {
            Proxy proxy = new Proxy(Proxy.Type.valueOf(proxyType),
                    new InetSocketAddress(proxyHost, proxyPort));
            builder.proxy(proxy);
        }

        if (storeHTTPHeaders) {
            builder.addNetworkInterceptor(new HTTPHeadersInterceptor());
        }

        if (ConfUtils.getBoolean(conf, "http.trust.everything", true)) {
            builder.sslSocketFactory(trustAllSslSocketFactory,
                    (X509TrustManager) trustAllCerts[0]);
            builder.hostnameVerifier(new HostnameVerifier() {
                @Override
                public boolean verify(String hostname, SSLSession session) {
                    return true;
                }
            });
        }

        client = builder.build();
    }

    private void addCookiesToRequest(Builder rb, String url, Metadata md) {
        String[] cookieStrings = md.getValues(RESPONSE_COOKIES_HEADER);
        if (cookieStrings == null || cookieStrings.length == 0) {
            return;
        }
        try {
            List<Cookie> cookies = CookieConverter.getCookies(cookieStrings,
                    new URL(url));
            for (Cookie c : cookies) {
                rb.addHeader("Cookie", c.getName() + "=" + c.getValue());
            }
        } catch (MalformedURLException e) { // Bad url , nothing to do
        }
    }

    @Override
    public ProtocolResponse getProtocolOutput(String url, final Metadata metadata) throws Exception {
        Builder rb = new Request.Builder().url(url);
        customRequestHeaders.forEach((k) -> {
            rb.header(k[0], k[1]);
        });

        if (metadata != null) {
            String lastModified = metadata.getFirstValue("last-modified");
            if (StringUtils.isNotBlank(lastModified)) {
                rb.header("If-Modified-Since",
                        HttpHeaders.formatHttpDate(lastModified));
            }

            String ifNoneMatch = metadata.getFirstValue("etag");
            if (StringUtils.isNotBlank(ifNoneMatch)) {
                rb.header("If-None-Match", ifNoneMatch);
            }

            String accept = metadata.getFirstValue("http.accept");
            if (StringUtils.isNotBlank(accept)) {
                rb.header("Accept", accept);
            }

            String acceptLanguage = metadata.getFirstValue("http.accept.language");
            if (StringUtils.isNotBlank(acceptLanguage)) {
                rb.header("Accept-Language", acceptLanguage);
            }

            if (useCookies) {
                addCookiesToRequest(rb, url, metadata);
            }
            
            String postJSONData = metadata.getFirstValue("http.post.json");
            if (StringUtils.isNotBlank(postJSONData)) {
                RequestBody body = RequestBody.create(JSON, postJSONData);
                rb.post(body);
            }
        }

        Request request = rb.build();
        Call call = client.newCall(request);

        try (Response response = call.execute()) {

            byte[] bytes = new byte[] {};

            Metadata responsemetadata = new Metadata();
            Headers headers = response.headers();

            for (int i = 0, size = headers.size(); i < size; i++) {
                String key = headers.name(i);
                String value = headers.value(i);

                if (key.equalsIgnoreCase(VERBATIM_REQUEST_KEY) || key.equalsIgnoreCase(VERBATIM_RESPONSE_KEY)) {
                    value = new String(Base64.getDecoder().decode(value));
                }

                responsemetadata.addValue(key.toLowerCase(Locale.ROOT), value);
            }

            MutableObject trimmed = new MutableObject(TrimmedContentReason.NOT_TRIMMED);
            bytes = toByteArray(response.body(), trimmed);
            if (trimmed.getValue() != TrimmedContentReason.NOT_TRIMMED) {
                if (!call.isCanceled()) {
                    call.cancel();
                }
                responsemetadata.setValue("http.trimmed", "true");
                responsemetadata.setValue("http.trimmed.reason",
                        trimmed.getValue().toString().toLowerCase(Locale.ROOT));
                LOG.warn("HTTP content trimmed to {}", bytes.length);
            }

            return new ProtocolResponse(bytes, response.code(), responsemetadata);
        }
    }

    private final byte[] toByteArray(final ResponseBody responseBody,
            MutableObject trimmed) throws IOException {

        if (responseBody == null) {
            return new byte[] {};
        }

        int maxContentBytes = Integer.MAX_VALUE;
        if (maxContent != -1) {
            maxContentBytes = Math.min(maxContentBytes, maxContent);
        }

        long endDueFor = -1;
        if (completionTimeout != -1) {
            endDueFor = System.currentTimeMillis() + (completionTimeout * 1000);
        }

        BufferedSource source = responseBody.source();
        int bytesRequested = 0;
        int bufferGrowStepBytes = 8192;

        while (source.buffer().size() < maxContentBytes) {
            bytesRequested += Math.min(bufferGrowStepBytes,
                    (maxContentBytes - bytesRequested));
            boolean success = false;
            try {
                success = source.request(bytesRequested);
            } catch (IOException e) {
                // requesting more content failed, e.g. by a socket timeout
                if (partialContentAsTrimmed && source.buffer().size() > 0) {
                    // treat already fetched content as trimmed
                    trimmed.setValue(TrimmedContentReason.DISCONNECT);
                    LOG.debug("Exception while fetching {}", e);
                } else {
                    throw e;
                }
            }
            if (!success) {
                // source exhausted, no more data to read
                break;
            }

            if (endDueFor != -1 && endDueFor <= System.currentTimeMillis()) {
                // check whether we hit the completion timeout
                trimmed.setValue(TrimmedContentReason.TIME);
                break;
            }

            // okhttp may fetch more content than requested, quickly "increment"
            // bytes
            bytesRequested = (int) source.buffer().size();
        }
        int bytesBuffered = (int) source.buffer().size();
        int bytesToCopy = bytesBuffered;
        if (maxContent != -1 && bytesToCopy > maxContent) {
            // okhttp's internal buffer is larger than maxContent
            trimmed.setValue(TrimmedContentReason.LENGTH);
            bytesToCopy = maxContentBytes;
        }
        byte[] arr = new byte[bytesToCopy];
        source.buffer().readFully(arr);
        return arr;
    }

    class HTTPHeadersInterceptor implements Interceptor {

        final ZoneId TIME_ZONE_UTC = ZoneId.of(ZoneOffset.UTC.toString());

        @Override
        public Response intercept(Interceptor.Chain chain) throws IOException {

            String startFetchTime = DateTimeFormatter.ISO_INSTANT
                    .format(ZonedDateTime.ofInstant(Instant.now(),
                            TIME_ZONE_UTC));

            Connection connection = chain.connection();
            String ipAddress = connection.socket().getInetAddress()
                    .getHostAddress();
            Request request = chain.request();

            StringBuilder requestverbatim = new StringBuilder();

            int position = request.url().toString()
                    .indexOf(request.url().host());
            String u = request.url().toString()
                    .substring(position + request.url().host().length());

            requestverbatim.append(request.method()).append(" ").append(u)
                    .append(" ").append("\r\n");

            Headers headers = request.headers();

            for (int i = 0, size = headers.size(); i < size; i++) {
                String key = headers.name(i);
                String value = headers.value(i);
                requestverbatim.append(key).append(": ").append(value)
                        .append("\r\n");
            }

            requestverbatim.append("\r\n");

            Response response = chain.proceed(request);

            StringBuilder responseverbatim = new StringBuilder();

            responseverbatim
                    .append(response.protocol().toString()
                            .toUpperCase(Locale.ROOT)).append(" ")
                    .append(response.code()).append(" ")
                    .append(response.message()).append("\r\n");

            headers = response.headers();

            for (int i = 0, size = headers.size(); i < size; i++) {
                String key = headers.name(i);
                String value = headers.value(i);
                responseverbatim.append(key).append(": ").append(value)
                        .append("\r\n");
            }

            responseverbatim.append("\r\n");

            byte[] encodedBytesResponse = Base64.getEncoder().encode(
                    responseverbatim.toString().getBytes());

            byte[] encodedBytesRequest = Base64.getEncoder().encode(
                    requestverbatim.toString().getBytes());

            // returns a modified version of the response
            return response
                    .newBuilder()
                    .header(VERBATIM_REQUEST_KEY,
                            new String(encodedBytesRequest))
                    .header(VERBATIM_RESPONSE_KEY,
                            new String(encodedBytesResponse))
                    .header(VERBATIM_RESPONSE_IP_KEY, ipAddress)
                    .header(VERBATIM_REQUEST_TIME_KEY, startFetchTime).build();
        }
    }

    public static void main(String args[]) throws Exception {
        HttpProtocol.main(new HttpProtocol(), args);
    }

}
