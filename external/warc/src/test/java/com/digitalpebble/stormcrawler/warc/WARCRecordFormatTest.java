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
package com.digitalpebble.stormcrawler.warc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.protocol.HttpHeaders;
import com.digitalpebble.stormcrawler.protocol.ProtocolResponse;
import java.nio.charset.StandardCharsets;
import org.apache.storm.tuple.Tuple;
import org.junit.Test;

public class WARCRecordFormatTest {

    private String protocolMDprefix = "http.";

    @Test
    public void testGetDigestSha1() {
        byte[] content = {'a', 'b', 'c', 'd', 'e', 'f'};
        String sha1str = "sha1:D6FMCDZDYW23YELHXWUEXAZ6LQCXU56S";
        assertEquals("Wrong sha1 digest", sha1str, WARCRecordFormat.getDigestSha1(content));
    }

    @Test
    public void testGetDigestSha1Empty() {
        byte[] content = {};
        String sha1str = "sha1:3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ";
        assertEquals("Wrong sha1 digest", sha1str, WARCRecordFormat.getDigestSha1(content));
    }

    @Test
    public void testGetDigestSha1TwoByteArrays() {
        byte[] content1 = {'a', 'b', 'c'};
        byte[] content2 = {'d', 'e', 'f'};
        String sha1str = "sha1:D6FMCDZDYW23YELHXWUEXAZ6LQCXU56S";
        assertEquals(
                "Wrong sha1 digest", sha1str, WARCRecordFormat.getDigestSha1(content1, content2));
    }

    @Test
    public void testGetDigestSha1RobotsTxt() {
        // trivial robots.txt file, sha1 from WARC file written by Nutch
        String robotsTxt = "User-agent: *\r\nDisallow:";
        byte[] content = robotsTxt.getBytes(StandardCharsets.UTF_8);
        String sha1str = "sha1:DHBVNHAJABWFHIYUHNCKYYIB3OBPFX3Y";
        assertEquals("Wrong sha1 digest", sha1str, WARCRecordFormat.getDigestSha1(content));
    }

    @Test
    public void testWarcRecord() {
        // test validity of WARC record
        String txt = "abcdef";
        byte[] content = txt.getBytes(StandardCharsets.UTF_8);
        String sha1str = "sha1:D6FMCDZDYW23YELHXWUEXAZ6LQCXU56S";
        Metadata metadata = new Metadata();
        metadata.addValue(
                protocolMDprefix + ProtocolResponse.RESPONSE_HEADERS_KEY,
                "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n");
        Tuple tuple = mock(Tuple.class);
        when(tuple.getBinaryByField("content")).thenReturn(content);
        when(tuple.getStringByField("url")).thenReturn("https://www.example.org/");
        when(tuple.getValueByField("metadata")).thenReturn(metadata);
        WARCRecordFormat format = new WARCRecordFormat(protocolMDprefix);
        byte[] warcBytes = format.format(tuple);
        String warcString = new String(warcBytes, StandardCharsets.UTF_8);
        // the WARC record has the form:
        // WARC header
        // \r\n\r\n
        // HTTP header
        // \r\n\r\n
        // payload
        // \r\n\r\n
        assertTrue(
                "WARC record: incorrect format of WARC header", warcString.startsWith("WARC/1.0"));
        assertTrue(
                "WARC record: incorrect format of HTTP header",
                warcString.contains("\r\n\r\nHTTP/1.1 200 OK\r\n"));
        assertTrue(
                "WARC record: single empty line between HTTP header and payload",
                warcString.contains("Content-Type: text/html\r\n\r\nabcdef"));
        assertTrue(
                "WARC record: record is required to end with \\r\\n\\r\\n",
                warcString.endsWith("\r\n\r\n"));
        assertTrue("WARC record: payload mangled", warcString.endsWith("\r\n\r\nabcdef\r\n\r\n"));
        assertTrue(
                "WARC record: no or incorrect payload digest",
                warcString.contains("\r\nWARC-Payload-Digest: " + sha1str + "\r\n"));
    }

    @Test
    public void testReplaceHeaders() {
        // test whether wrong/misleading HTTP headers are replaced
        // because payload is not saved in original Content-Encoding and
        // Transfer-Encoding
        String txt = "abcdef";
        byte[] content = txt.getBytes(StandardCharsets.UTF_8);
        String sha1str = "sha1:D6FMCDZDYW23YELHXWUEXAZ6LQCXU56S";
        Metadata metadata = new Metadata();
        metadata.addValue(
                protocolMDprefix + ProtocolResponse.RESPONSE_HEADERS_KEY, //
                "HTTP/1.1 200 OK\r\n" //
                        + "Content-Type: text/html\r\n" //
                        + "Content-Encoding: gzip\r\n" //
                        + "Content-Length: 26\r\n" //
                        + "Connection: close");
        metadata.addValue(protocolMDprefix + ProtocolResponse.RESPONSE_IP_KEY, "123.123.123.123");
        Tuple tuple = mock(Tuple.class);
        when(tuple.getBinaryByField("content")).thenReturn(content);
        when(tuple.getStringByField("url")).thenReturn("https://www.example.org/");
        when(tuple.getValueByField("metadata")).thenReturn(metadata);
        WARCRecordFormat format = new WARCRecordFormat(protocolMDprefix);
        byte[] warcBytes = format.format(tuple);
        String warcString = new String(warcBytes, StandardCharsets.UTF_8);
        assertFalse(
                "WARC record: HTTP header Content-Encoding not replaced",
                warcString.contains("\r\nContent-Encoding: gzip\r\n"));
        assertFalse(
                "WARC record: HTTP header Content-Length not replaced",
                warcString.contains("\r\nContent-Length: 26\r\n"));
        // the correct Content-Length is 6 (txt = "abcdef")
        assertTrue(
                "WARC record: HTTP header Content-Length does not match payload length",
                warcString.contains("\r\nContent-Length: 6\r\n"));
        assertTrue(
                "WARC record: HTTP header does not end with \\r\\n\\r\\n",
                warcString.contains("\r\nConnection: close\r\n\r\nabcdef"));
    }

    @Test
    public void testReplaceHttpVersion() {
        /*
         * Some WARC readers only accept "HTTP/1.0" or "HTTP/1.1" as HTTP protocol identifier in
         * HTTP request and status lines. Any other protocol versions should be replaced by one of
         * the mentioned commonly accepted protocol version strings.
         */
        String txt = "abcdef";
        byte[] content = txt.getBytes(StandardCharsets.UTF_8);
        Metadata metadata = new Metadata();
        metadata.addValue(
                protocolMDprefix + ProtocolResponse.RESPONSE_HEADERS_KEY, //
                "HTTP/2 200 OK\r\n" //
                        + "Content-Type: text/html\r\n" //
                        + "Content-Encoding: gzip\r\n" //
                        + "Content-Length: 26\r\n" //
                        + "Connection: close");
        metadata.addValue(
                protocolMDprefix + ProtocolResponse.PROTOCOL_VERSIONS_KEY,
                "h2,TLS_1_3,TLS_AES_256_GCM_SHA384");
        metadata.addValue(protocolMDprefix + ProtocolResponse.RESPONSE_IP_KEY, "123.123.123.123");
        Tuple tuple = mock(Tuple.class);
        when(tuple.getBinaryByField("content")).thenReturn(content);
        when(tuple.getStringByField("url")).thenReturn("https://www.example.org/");
        when(tuple.getValueByField("metadata")).thenReturn(metadata);
        WARCRecordFormat format = new WARCRecordFormat(protocolMDprefix);
        byte[] warcBytes = format.format(tuple);
        String warcString = new String(warcBytes, StandardCharsets.UTF_8);
        String[] headersPayload = warcString.split("\r\n\r\n");
        assertEquals(
                "WARC response record must include WARC header, HTTP header and payload",
                3,
                headersPayload.length);
        String statusLine = headersPayload[1].split("\r\n", 2)[0];
        assertTrue(
                "WARC response record: HTTP status line must start with HTTP/1.1 or HTTP/1.0",
                statusLine.matches("^HTTP/1\\.[01] .*"));
        assertTrue(
                "WARC response record is expected to include WARC header \"WARC-Protocol\"",
                headersPayload[0].contains("\r\nWARC-Protocol: "));
        assertTrue(
                "WARC response record is expected to include WARC header \"WARC-IP-Address\"",
                headersPayload[0].contains("\r\nWARC-IP-Address: "));
    }

    @Test
    public void testRequestHeader() {
        String txt = "abcdef";
        byte[] content = txt.getBytes(StandardCharsets.UTF_8);
        Metadata metadata = new Metadata();
        metadata.addValue(
                protocolMDprefix + ProtocolResponse.REQUEST_HEADERS_KEY, //
                "GET / HTTP/2\r\n" //
                        + "User-Agent: mybot\r\n" //
                        + "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8\r\n" //
                        + "Accept-Language: en-us,en-gb,en;q=0.7,*;q=0.3\r\n" //
                        + "Accept-Encoding: br,gzip\r\n" //
                        + "Connection: Keep-Alive\r\n\r\n");
        metadata.addValue(protocolMDprefix + ProtocolResponse.RESPONSE_IP_KEY, "123.123.123.123");
        Tuple tuple = mock(Tuple.class);
        when(tuple.getBinaryByField("content")).thenReturn(content);
        when(tuple.getStringByField("url")).thenReturn("https://www.example.org/");
        when(tuple.getValueByField("metadata")).thenReturn(metadata);
        WARCRecordFormat format = new WARCRequestRecordFormat(protocolMDprefix);
        byte[] warcBytes = format.format(tuple);
        String warcString = new String(warcBytes, StandardCharsets.UTF_8);
        String[] headers = warcString.split("\r\n\r\n");
        assertEquals("WARC request record must include WARC and HTTP header", 2, headers.length);
        String requestLine = headers[1].split("\r\n", 2)[0];
        assertTrue(
                "WARC request record: HTTP request line must end with HTTP/1.1 or HTTP/1.0",
                requestLine.matches(".* HTTP/1\\.[01]$"));
    }

    @Test
    public void testWarcDateFormat() {
        Metadata metadata = new Metadata();
        /*
         * To meet the WARC 1.0 standard the ISO date format with seconds
         * precision (not milliseconds) is expected. We pass epoch millisecond 1
         * to the formatter to ensure that the precision isn't increased on
         * demand (as by the ISO_INSTANT formatter):
         */
        metadata.addValue(protocolMDprefix + ProtocolResponse.REQUEST_TIME_KEY, "1");
        WARCRecordFormat format = new WARCRecordFormat(protocolMDprefix);
        assertEquals("1970-01-01T00:00:00Z", format.getCaptureTime(metadata));
    }

    @Test
    public void testWarcResourceRecord() {
        // test writing of WARC resource record (no verbatim HTTP headers stored,
        // `http.store.headers: false`)
        String txt = "abcdef";
        byte[] content = txt.getBytes(StandardCharsets.UTF_8);
        String sha1str = "sha1:D6FMCDZDYW23YELHXWUEXAZ6LQCXU56S";
        Metadata metadata = new Metadata();
        metadata.addValue(protocolMDprefix + HttpHeaders.CONTENT_TYPE, "text/html");
        Tuple tuple = mock(Tuple.class);
        when(tuple.getBinaryByField("content")).thenReturn(content);
        when(tuple.getStringByField("url")).thenReturn("https://www.example.org/");
        when(tuple.getValueByField("metadata")).thenReturn(metadata);
        WARCRecordFormat format = new WARCRecordFormat(protocolMDprefix);
        byte[] warcBytes = format.format(tuple);
        String warcString = new String(warcBytes, StandardCharsets.UTF_8);
        assertTrue(
                "WARC record: incorrect format of WARC header", warcString.startsWith("WARC/1.0"));
        assertTrue(
                "WARC record: record is required to end with \\r\\n\\r\\n",
                warcString.endsWith("\r\n\r\n"));
        assertTrue(
                "WARC record: record type must be \"resource\"",
                warcString.contains("WARC-Type: resource\r\n"));
        assertTrue(
                "WARC record: Content-Type should be \"text/html\"",
                warcString.contains("Content-Type: text/html\r\n"));
        assertTrue(
                "WARC record: no or incorrect payload digest",
                warcString.contains("\r\nWARC-Payload-Digest: " + sha1str + "\r\n"));
        assertTrue(
                "WARC record: no or incorrect block, digest",
                warcString.contains("\r\nWARC-Block-Digest: " + sha1str + "\r\n"));
    }
}
