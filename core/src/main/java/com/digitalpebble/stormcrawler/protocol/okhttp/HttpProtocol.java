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
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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

import com.digitalpebble.stormcrawler.Constants;
import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.protocol.AbstractHttpProtocol;
import com.digitalpebble.stormcrawler.protocol.HttpHeaders;
import com.digitalpebble.stormcrawler.protocol.ProtocolResponse;
import com.digitalpebble.stormcrawler.protocol.ProtocolResponse.TrimmedContentReason;
import com.digitalpebble.stormcrawler.proxy.SCProxy;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import com.digitalpebble.stormcrawler.util.CookieConverter;

import okhttp3.Call;
import okhttp3.Connection;
import okhttp3.Credentials;
import okhttp3.EventListener;
import okhttp3.EventListener.Factory;
import okhttp3.Headers;
import okhttp3.Interceptor;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Request.Builder;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okhttp3.Route;
import okhttp3.brotli.BrotliInterceptor;
import okio.BufferedSource;

public class HttpProtocol extends AbstractHttpProtocol {

    private static final org.slf4j.Logger LOG = LoggerFactory
            .getLogger(HttpProtocol.class);

    private final MediaType JSON = MediaType
            .parse("application/json; charset=utf-8");

    private OkHttpClient client;

    private int globalMaxContent;

    private int completionTimeout = -1;

    /** Accept partially fetched content as trimmed content */
    private boolean partialContentAsTrimmed = false;

    private final List<KeyValue> customRequestHeaders = new LinkedList<>();

    // track the time spent for each URL in DNS resolution
    private final Map<String, Long> DNStimes = new HashMap<>();

    private OkHttpClient.Builder builder;

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

        globalMaxContent = ConfUtils.getInt(conf, "http.content.limit", -1);

        int timeout = ConfUtils.getInt(conf, "http.timeout", 10000);

        this.completionTimeout = ConfUtils.getInt(conf,
                "topology.message.timeout.secs", completionTimeout);

        this.partialContentAsTrimmed = ConfUtils.getBoolean(conf,
                "http.content.partial.as.trimmed", false);

        builder = new OkHttpClient.Builder()
                .retryOnConnectionFailure(true).followRedirects(false)
                .connectTimeout(timeout, TimeUnit.MILLISECONDS)
                .writeTimeout(timeout, TimeUnit.MILLISECONDS)
                .readTimeout(timeout, TimeUnit.MILLISECONDS);

        // protocols in order of preference, see
        // https://square.github.io/okhttp/4.x/okhttp/okhttp3/-ok-http-client/-builder/protocols/
        List<okhttp3.Protocol> protocols = new ArrayList<>();
        for (String pVersion : protocolVersions) {
            switch (pVersion) {
            case "h2":
                protocols.add(okhttp3.Protocol.HTTP_2);
                break;
            case "h2c":
                if (protocolVersions.size() > 1) {
                    LOG.error(
                            "h2c ignored, it cannot be combined with any other protocol");
                } else {
                    protocols.add(okhttp3.Protocol.H2_PRIOR_KNOWLEDGE);
                }
                break;
            case "http/1.1":
                protocols.add(okhttp3.Protocol.HTTP_1_1);
                break;
            case "http/1.0":
                LOG.warn(
                        "http/1.0 ignored, not supported by okhttp for requests");
                break;
            default:
                LOG.error("{}: unknown protocol version", pVersion);
                break;
            }
        }
        if (protocols.size() > 0) {
            LOG.info("Using protocol versions: {}", protocols);
            builder.protocols(protocols);
        }

        String userAgent = getAgentString(conf);
        if (StringUtils.isNotBlank(userAgent)) {
            customRequestHeaders.add(new KeyValue("User-Agent", userAgent));
        }

        String accept = ConfUtils.getString(conf, "http.accept");
        if (StringUtils.isNotBlank(accept)) {
            customRequestHeaders.add(new KeyValue("Accept", accept));
        }

        String acceptLanguage = ConfUtils.getString(conf,
                "http.accept.language");
        if (StringUtils.isNotBlank(acceptLanguage)) {
            customRequestHeaders
                    .add(new KeyValue("Accept-Language", acceptLanguage));
        }

        String basicAuthUser = ConfUtils.getString(conf, "http.basicauth.user",
                null);

        // use a basic auth?
        if (StringUtils.isNotBlank(basicAuthUser)) {
            String basicAuthPass = ConfUtils.getString(conf,
                    "http.basicauth.password", "");
            String encoding = Base64.getEncoder().encodeToString(
                    (basicAuthUser + ":" + basicAuthPass).getBytes());
            customRequestHeaders
                    .add(new KeyValue("Authorization", "Basic " + encoding));
        }
        
        customHeaders.forEach(customRequestHeaders::add);

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

        builder.eventListenerFactory(new Factory() {
            @Override
            public EventListener create(Call call) {
                return new DNSResolutionListener(DNStimes);
            }
        });

        // enable support for Brotli compression (Content-Encoding)
        builder.addInterceptor(BrotliInterceptor.INSTANCE);

        client = builder.build();
    }

    private void addCookiesToRequest(Builder rb, String url, Metadata md) {
        String[] cookieStrings = md.getValues(RESPONSE_COOKIES_HEADER,
                protocolMDprefix);
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
        // create default local client
        OkHttpClient localClient = client;

        // conditionally add a dynamic proxy
        if (proxyManager != null) {
            // retrieve proxy from proxy manager
            SCProxy prox = proxyManager.getProxy(metadata);

            // conditionally configure proxy authentication
            if (StringUtils.isNotBlank(prox.getAddress())) {
                // format SCProxy into native Java proxy
                Proxy proxy = new Proxy(Proxy.Type.valueOf(prox.getProtocol().toUpperCase()),
                        new InetSocketAddress(prox.getAddress(), Integer.parseInt(prox.getPort())));

                // set proxy in builder
                builder.proxy(proxy);

                // conditionally add proxy authentication
                if (StringUtils.isNotBlank(prox.getUsername())) {
                    // add proxy authentication header to builder
                    builder.proxyAuthenticator((Route route, Response response) -> {
                        String credential = Credentials.basic(prox.getUsername(),
                                prox.getPassword());
                        return response.request().newBuilder()
                                .header("Proxy-Authorization", credential).build();
                    });
                }
            }

            // save start time for debugging speed impact of client build
            long buildStart = System.currentTimeMillis();

            // create new local client from builder using proxy
            localClient = builder.build();

            LOG.debug("time to build okhttp client with proxy: {}ms", System.currentTimeMillis() - buildStart);

            LOG.debug("fetching with proxy {} - {} ", url, prox.toString());
        }

        Builder rb = new Request.Builder().url(url);
        customRequestHeaders.forEach((k) -> {
            rb.header(k.getKey(), k.getValue());
        });

        int pageMaxContent = globalMaxContent;

        if (metadata != null) {
            String lastModified = metadata
                    .getFirstValue(HttpHeaders.LAST_MODIFIED);
            if (StringUtils.isNotBlank(lastModified)) {
                rb.header("If-Modified-Since",
                        HttpHeaders.formatHttpDate(lastModified));
            }

            String ifNoneMatch = metadata.getFirstValue("etag",
                    protocolMDprefix);
            if (StringUtils.isNotBlank(ifNoneMatch)) {
                rb.header("If-None-Match", ifNoneMatch);
            }

            String accept = metadata.getFirstValue("http.accept");
            if (StringUtils.isNotBlank(accept)) {
                rb.header("Accept", accept);
            }

            String acceptLanguage = metadata
                    .getFirstValue("http.accept.language");
            if (StringUtils.isNotBlank(acceptLanguage)) {
                rb.header("Accept-Language", acceptLanguage);
            }

            String pageMaxContentStr = metadata.getFirstValue("http.content.limit");
            if (StringUtils.isNotBlank(pageMaxContentStr)) {
                try {
                    pageMaxContent = Integer.parseInt(pageMaxContentStr);
                } catch (NumberFormatException e) {
                    LOG.warn("Invalid http.content.limit in metadata: {}", pageMaxContentStr);
                }
            }

            if (useCookies) {
                addCookiesToRequest(rb, url, metadata);
            }

            String postJSONData = metadata.getFirstValue("http.post.json");
            if (StringUtils.isNotBlank(postJSONData)) {
                RequestBody body = RequestBody.create(postJSONData, JSON);
                rb.post(body);
            }

            String useHead = metadata.getFirstValue("http.method.head");
            if ("true".equalsIgnoreCase(useHead)) {
                rb.head();
            }
        }

        Request request = rb.build();

        Call call = localClient.newCall(request);

        try (Response response = call.execute()) {

            byte[] bytes = new byte[] {};

            Metadata responsemetadata = new Metadata();
            Headers headers = response.headers();

            for (int i = 0, size = headers.size(); i < size; i++) {
                String key = headers.name(i);
                String value = headers.value(i);

                if (key.equals(ProtocolResponse.REQUEST_HEADERS_KEY)
                        || key.equals(ProtocolResponse.RESPONSE_HEADERS_KEY)) {
                    value = new String(Base64.getDecoder().decode(value));
                }

                responsemetadata.addValue(key.toLowerCase(Locale.ROOT), value);
            }

            MutableObject trimmed = new MutableObject(
                    TrimmedContentReason.NOT_TRIMMED);
            bytes = toByteArray(response.body(), pageMaxContent, trimmed);
            if (trimmed.getValue() != TrimmedContentReason.NOT_TRIMMED) {
                if (!call.isCanceled()) {
                    call.cancel();
                }
                responsemetadata.setValue(ProtocolResponse.TRIMMED_RESPONSE_KEY,
                        "true");
                responsemetadata.setValue(
                        ProtocolResponse.TRIMMED_RESPONSE_REASON_KEY,
                        trimmed.getValue().toString().toLowerCase(Locale.ROOT));
                LOG.warn("HTTP content trimmed to {}", bytes.length);
            }

            Long DNSResolution = DNStimes.remove(call.toString());
            if (DNSResolution != null) {
                responsemetadata.setValue("metrics.dns.resolution.msec",
                        DNSResolution.toString());
            }

            return new ProtocolResponse(bytes, response.code(),
                    responsemetadata);
        }
    }

    private final byte[] toByteArray(final ResponseBody responseBody,
            int maxContent, MutableObject trimmed) throws IOException {

        if (responseBody == null) {
            return new byte[] {};
        }

        int maxContentBytes = Constants.MAX_ARRAY_SIZE;
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

        while (source.getBuffer().size() <= maxContentBytes) {
            bytesRequested += Math.min(bufferGrowStepBytes,
            /*
             * request one byte more than required to reliably detect truncated
             * content, but beware of integer overflows
             */
            (maxContentBytes == Constants.MAX_ARRAY_SIZE ? maxContentBytes
                    : (1 + maxContentBytes)) - bytesRequested);
            boolean success = false;
            try {
                success = source.request(bytesRequested);
            } catch (IOException e) {
                // requesting more content failed, e.g. by a socket timeout
                if (partialContentAsTrimmed && source.getBuffer().size() > 0) {
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
            bytesRequested = (int) source.getBuffer().size();
        }
        int bytesBuffered = (int) source.getBuffer().size();
        int bytesToCopy = bytesBuffered;
        if (maxContent != -1 && bytesToCopy > maxContent) {
            // okhttp's internal buffer is larger than maxContent
            trimmed.setValue(TrimmedContentReason.LENGTH);
            bytesToCopy = maxContentBytes;
        }
        byte[] arr = new byte[bytesToCopy];
        source.getBuffer().readFully(arr);
        return arr;
    }

    class HTTPHeadersInterceptor implements Interceptor {

        private String getNormalizedProtocolName(Protocol protocol) {
            String name = protocol.toString().toUpperCase(Locale.ROOT);
            if ("H2".equals(name)) {
                // back-ward compatible protocol version name
                name = "HTTP/2";
            }
            return name;
        }

        @Override
        public Response intercept(Interceptor.Chain chain) throws IOException {

            long startFetchTime = System.currentTimeMillis();

            Connection connection = chain.connection();
            String ipAddress = connection.socket().getInetAddress()
                    .getHostAddress();
            Request request = chain.request();

            StringBuilder requestverbatim = new StringBuilder();

            int position = request.url().toString()
                    .indexOf(request.url().host());
            String u = request.url().toString()
                    .substring(position + request.url().host().length());

            String httpProtocol = getNormalizedProtocolName(connection
                    .protocol());

            requestverbatim.append(request.method()).append(" ").append(u)
                    .append(" ").append(httpProtocol).append("\r\n");

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

            /*
             * Note: the protocol version between request and response may
             * differ, a server may respond with HTTP/1.0 on a HTTP/1.1 request
             */
            httpProtocol = getNormalizedProtocolName(response.protocol());

            responseverbatim.append(httpProtocol).append(" ")
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
                    .header(ProtocolResponse.REQUEST_HEADERS_KEY,
                            new String(encodedBytesRequest))
                    .header(ProtocolResponse.RESPONSE_HEADERS_KEY,
                            new String(encodedBytesResponse))
                    .header(ProtocolResponse.RESPONSE_IP_KEY, ipAddress)
                    .header(ProtocolResponse.REQUEST_TIME_KEY,
                            Long.toString(startFetchTime)).build();
        }
    }

    public static void main(String args[]) throws Exception {
        HttpProtocol.main(new HttpProtocol(), args);
    }

}
