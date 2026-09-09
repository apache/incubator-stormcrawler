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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.zip.GZIPOutputStream;
import org.apache.storm.Config;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.protocol.AbstractProtocolTest;
import org.apache.stormcrawler.protocol.ProtocolResponse;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.junit.jupiter.api.Test;

/**
 * Content limit applied by the OkHttp protocol, including the limit passed through the metadata by
 * the robots.txt fetch.
 */
class HttpProtocolContentLimitTest extends AbstractProtocolTest {

    @Override
    protected Handler[] getHandlers() {
        return new Handler[] {new SizedContentHandler()};
    }

    @Test
    void metadataLimitOfMinusOneDoesNotRemoveTheGlobalLimit() throws Exception {
        final ProtocolResponse response = fetch(protocol(1024), "/plain/4096", "-1");
        assertEquals(1024, response.getContent().length);
        assertTrimmedForLength(response);
    }

    @Test
    void metadataLimitBelowMinusOneIsIgnored() throws Exception {
        final ProtocolResponse response = fetch(protocol(1024), "/plain/4096", "-2");
        assertEquals(1024, response.getContent().length);
        assertTrimmedForLength(response);
    }

    @Test
    void nonNumericMetadataLimitIsIgnored() throws Exception {
        final ProtocolResponse response = fetch(protocol(1024), "/plain/4096", "not a number");
        assertEquals(1024, response.getContent().length);
        assertTrimmedForLength(response);
    }

    @Test
    void metadataLimitMayExceedTheGlobalLimit() throws Exception {
        final ProtocolResponse response = fetch(protocol(1024), "/plain/2048", "4096");
        assertEquals(2048, response.getContent().length);
        assertNull(response.getMetadata().getFirstValue(ProtocolResponse.TRIMMED_RESPONSE_KEY));
    }

    @Test
    void metadataLimitMayTightenTheGlobalLimit() throws Exception {
        final ProtocolResponse response = fetch(protocol(4096), "/plain/2048", "512");
        assertEquals(512, response.getContent().length);
        assertTrimmedForLength(response);
    }

    @Test
    void metadataLimitOfMinusOneAppliesWhenTheGlobalLimitIsUnlimited() throws Exception {
        final ProtocolResponse response = fetch(protocol(-1), "/plain/2048", "-1");
        assertEquals(2048, response.getContent().length);
        assertNull(response.getMetadata().getFirstValue(ProtocolResponse.TRIMMED_RESPONSE_KEY));
    }

    @Test
    void noMetadataLimitLeavesTheGlobalLimitInPlace() throws Exception {
        final ProtocolResponse response = fetch(protocol(1024), "/plain/4096", null);
        assertEquals(1024, response.getContent().length);
        assertTrimmedForLength(response);
    }

    @Test
    void gzippedResponseIsTrimmedOnItsDecompressedLength() throws Exception {
        final ProtocolResponse response = fetch(protocol(1024), "/gzip/4096", "-1");
        assertEquals(1024, response.getContent().length);
        assertTrimmedForLength(response);
    }

    @Test
    void gzippedResponseIsReadInFullUpToAWidenedLimit() throws Exception {
        final ProtocolResponse response = fetch(protocol(1024), "/gzip/4096", "8192");
        assertEquals(4096, response.getContent().length);
        assertNull(response.getMetadata().getFirstValue(ProtocolResponse.TRIMMED_RESPONSE_KEY));
    }

    private void assertTrimmedForLength(ProtocolResponse response) {
        final Metadata md = response.getMetadata();
        assertEquals("true", md.getFirstValue(ProtocolResponse.TRIMMED_RESPONSE_KEY));
        assertEquals("length", md.getFirstValue(ProtocolResponse.TRIMMED_RESPONSE_REASON_KEY));
    }

    /** Fetches a path, optionally passing http.content.limit through the metadata. */
    private ProtocolResponse fetch(HttpProtocol protocol, String path, String metadataLimit)
            throws Exception {
        final Metadata md = new Metadata();
        if (metadataLimit != null) {
            md.setValue("http.content.limit", metadataLimit);
        }
        return protocol.getProtocolOutput("http://localhost:" + HTTP_PORT + path, md);
    }

    private HttpProtocol protocol(int globalContentLimit) {
        final Config conf = new Config();
        conf.put("http.agent.name", "test");
        conf.put("http.content.limit", globalContentLimit);
        final HttpProtocol protocol = new HttpProtocol();
        protocol.configure(conf);
        return protocol;
    }

    /** Serves /plain/&lt;n&gt; and /gzip/&lt;n&gt; as n bytes of uncompressed content. */
    static class SizedContentHandler extends AbstractHandler {

        @Override
        public void handle(
                String target,
                Request baseRequest,
                HttpServletRequest request,
                HttpServletResponse response)
                throws IOException {
            baseRequest.setHandled(true);
            final String[] segments = target.split("/");
            final int size = Integer.parseInt(segments[2]);
            final byte[] content = new byte[size];
            Arrays.fill(content, (byte) 'a');
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType("text/plain; charset=UTF-8");
            final byte[] body;
            if (segments[1].equals("gzip")) {
                final ByteArrayOutputStream compressed = new ByteArrayOutputStream();
                try (GZIPOutputStream gzip = new GZIPOutputStream(compressed)) {
                    gzip.write(content);
                }
                body = compressed.toByteArray();
                response.setHeader("Content-Encoding", "gzip");
            } else {
                body = content;
            }
            response.setContentLength(body.length);
            try (OutputStream out = response.getOutputStream()) {
                out.write(body);
            }
        }
    }
}
