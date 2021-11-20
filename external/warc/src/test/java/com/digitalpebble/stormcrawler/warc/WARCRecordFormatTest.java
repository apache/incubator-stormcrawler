package com.digitalpebble.stormcrawler.warc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.digitalpebble.stormcrawler.Metadata;
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
}
