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
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.security.cert.CertificateException;
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
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.http.util.ByteArrayBuffer;
import org.apache.storm.Config;
import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.protocol.AbstractHttpProtocol;
import com.digitalpebble.stormcrawler.protocol.ProtocolResponse;
import com.digitalpebble.stormcrawler.util.ConfUtils;

import okhttp3.Call;
import okhttp3.Headers;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Request.Builder;
import okhttp3.Response;
import okhttp3.ResponseBody;

public class HttpProtocol extends AbstractHttpProtocol {

    private static final org.slf4j.Logger LOG = LoggerFactory
            .getLogger(HttpProtocol.class);

    private OkHttpClient client;

    private int maxContent;

    private int completionTimeout = -1;

    private final static String VERBATIM_REQUEST_KEY = "_request.headers_";
    private final static String VERBATIM_RESPONSE_KEY = "_response.headers_";

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

    @Override
    public ProtocolResponse getProtocolOutput(String url, final Metadata metadata) throws Exception {
        Builder rb = new Request.Builder().url(url);

        customRequestHeaders.forEach((k) -> {
            rb.header(k[0], k[1]);
        });

        if (metadata != null) {
            String lastModified = metadata.getFirstValue("last-modified");
            if (StringUtils.isNotBlank(lastModified)) {
                rb.header("If-Modified-Since", lastModified);
            }

            String ifNoneMatch = metadata.getFirstValue("etag");
            if (StringUtils.isNotBlank(ifNoneMatch)) {
                rb.header("If-None-Match", ifNoneMatch);
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

            MutableBoolean trimmed = new MutableBoolean();
            bytes = toByteArray(response.body(), trimmed);
            if (trimmed.booleanValue()) {
                if (!call.isCanceled()) {
                    call.cancel();
                }
                responsemetadata.setValue("http.trimmed", "true");
                LOG.warn("HTTP content trimmed to {}", bytes.length);
            }

            return new ProtocolResponse(bytes, response.code(), responsemetadata);
        }
    }

    private final byte[] toByteArray(final ResponseBody responseBody,
            MutableBoolean trimmed) throws IOException {

        if (responseBody == null)
            return new byte[] {};

        final InputStream instream = responseBody.byteStream();
        if (instream == null) {
            return null;
        }
        if (responseBody.contentLength() > Integer.MAX_VALUE) {
            throw new IOException(
                    "Cannot buffer entire body for content length: "
                            + responseBody.contentLength());
        }
        int reportedLength = (int) responseBody.contentLength();
        // set default size for buffer: 100 KB
        int bufferInitSize = 102400;
        if (reportedLength != -1) {
            bufferInitSize = reportedLength;
        }
        // avoid init of too large a buffer when we will trim anyway
        if (maxContent != -1 && bufferInitSize > maxContent) {
            bufferInitSize = maxContent;
        }
        long endDueFor = -1;
        if (completionTimeout != -1) {
            endDueFor = System.currentTimeMillis() + (completionTimeout * 1000);
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
            // check whether we hit the completion timeout
            if (endDueFor != -1 && endDueFor <= System.currentTimeMillis()) {
                trimmed.setValue(true);
                break;
            }
        }
        return buffer.toByteArray();
    }

    class HTTPHeadersInterceptor implements Interceptor {

        @Override
        public Response intercept(Interceptor.Chain chain) throws IOException {
            Request request = chain.request();

            StringBuilder resquestverbatim = new StringBuilder();

            int position = request.url().toString()
                    .indexOf(request.url().host());
            String u = request.url().toString()
                    .substring(position + request.url().host().length());

            resquestverbatim.append(request.method()).append(" ").append(u)
                    .append(" ").append("\r\n");

            Headers headers = request.headers();

            for (int i = 0, size = headers.size(); i < size; i++) {
                String key = headers.name(i);
                String value = headers.value(i);
                resquestverbatim.append(key).append(": ").append(value)
                        .append("\r\n");
            }

            resquestverbatim.append("\r\n");

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
                    resquestverbatim.toString().getBytes());

            // returns a modified version of the response
            return response
                    .newBuilder()
                    .header(VERBATIM_REQUEST_KEY,
                            new String(encodedBytesRequest))
                    .header(VERBATIM_RESPONSE_KEY,
                            new String(encodedBytesResponse)).build();
        }
    }

    public static void main(String args[]) throws Exception {
        HttpProtocol.main(new HttpProtocol(), args);
    }

}
