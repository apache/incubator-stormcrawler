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
import java.util.concurrent.TimeUnit;

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
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Request.Builder;
import okhttp3.Response;
import okhttp3.ResponseBody;

public class HttpProtocol extends AbstractHttpProtocol {

    private static final org.slf4j.Logger LOG = LoggerFactory
            .getLogger(HttpProtocol.class);

    private OkHttpClient client;

    private String userAgent;

    private int maxContent;

    private int completionTimeout = -1;

    @Override
    public void configure(Config conf) {
        super.configure(conf);

        this.maxContent = ConfUtils.getInt(conf, "http.content.limit", -1);

        int timeout = ConfUtils.getInt(conf, "http.timeout", 10000);

        this.completionTimeout = ConfUtils.getInt(conf,
                "topology.message.timeout.secs", completionTimeout);

        userAgent = getAgentString(conf);

        okhttp3.OkHttpClient.Builder builder = new OkHttpClient.Builder()
                .retryOnConnectionFailure(true).followRedirects(false)
                .connectTimeout(timeout, TimeUnit.MILLISECONDS)
                .writeTimeout(timeout, TimeUnit.MILLISECONDS)
                .readTimeout(timeout, TimeUnit.MILLISECONDS);

        String proxyHost = ConfUtils.getString(conf, "http.proxy.host", null);
        int proxyPort = ConfUtils.getInt(conf, "http.proxy.port", 8080);

        boolean useProxy = proxyHost != null && proxyHost.length() > 0;

        // use a proxy?
        if (useProxy) {
            Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(
                    proxyHost, proxyPort));
            builder.proxy(proxy);
        }

        client = builder.build();
    }

    @Override
    public ProtocolResponse getProtocolOutput(String url, Metadata metadata)
            throws Exception {
        Builder rb = new Request.Builder().url(url);
        rb.header("User-Agent", userAgent);

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

            MutableBoolean trimmed = new MutableBoolean();
            bytes = toByteArray(response.body(), trimmed);
            if (trimmed.booleanValue()) {
                if (!call.isCanceled()) {
                    call.cancel();
                }
                metadata.setValue("http.trimmed", "true");
                LOG.warn("HTTP content trimmed to {}", bytes.length);
            }

            return new ProtocolResponse(bytes, response.code(), metadata);
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

    public static void main(String args[]) throws Exception {
        HttpProtocol.main(new HttpProtocol(), args);
    }

}
