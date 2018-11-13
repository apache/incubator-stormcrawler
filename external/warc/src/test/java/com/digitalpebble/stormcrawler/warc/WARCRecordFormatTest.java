package com.digitalpebble.stormcrawler.warc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;

import org.apache.storm.tuple.Tuple;
import org.junit.Test;

import com.digitalpebble.stormcrawler.Metadata;

public class WARCRecordFormatTest {

    @Test
    public void testGetDigestSha1() {
        byte[] content = { 'a', 'b', 'c', 'd', 'e', 'f' };
        String sha1str = "sha1:D6FMCDZDYW23YELHXWUEXAZ6LQCXU56S";
        assertEquals("Wrong sha1 digest", sha1str,
                WARCRecordFormat.getDigestSha1(content));
    }

    @Test
    public void testGetDigestSha1Empty() {
        byte[] content = {};
        String sha1str = "sha1:3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ";
        assertEquals("Wrong sha1 digest", sha1str,
                WARCRecordFormat.getDigestSha1(content));
    }

    @Test
    public void testGetDigestSha1TwoByteArrays() {
        byte[] content1 = { 'a', 'b', 'c' };
        byte[] content2 = { 'd', 'e', 'f' };
        String sha1str = "sha1:D6FMCDZDYW23YELHXWUEXAZ6LQCXU56S";
        assertEquals("Wrong sha1 digest", sha1str,
                WARCRecordFormat.getDigestSha1(content1, content2));
    }

    @Test
    public void testGetDigestSha1RobotsTxt() {
        // trivial robots.txt file, sha1 from WARC file written by Nutch
        String robotsTxt = "User-agent: *\r\nDisallow:";
        byte[] content = robotsTxt.getBytes(StandardCharsets.UTF_8);
        String sha1str = "sha1:DHBVNHAJABWFHIYUHNCKYYIB3OBPFX3Y";
        assertEquals("Wrong sha1 digest", sha1str,
                WARCRecordFormat.getDigestSha1(content));
    }

    @Test
    public void testWarcRecord() {
        // test validity of WARC record
        String txt = "abcdef";
        byte[] content = txt.getBytes(StandardCharsets.UTF_8);
        String sha1str = "sha1:D6FMCDZDYW23YELHXWUEXAZ6LQCXU56S";
        Metadata metadata = new Metadata();
        metadata.addValue("_response.headers_",
                "HTTP/1.1 200 OK\r\nContent-Type: text/html");
        Tuple tuple = mock(Tuple.class);
        when(tuple.getBinaryByField("content")).thenReturn(content);
        when(tuple.getStringByField("url")).thenReturn(
                "https://www.example.org/");
        when(tuple.getValueByField("metadata")).thenReturn(metadata);
        WARCRecordFormat format = new WARCRecordFormat();
        byte[] warcBytes = format.format(tuple);
        String warcString = new String(warcBytes, StandardCharsets.UTF_8);
        // the WARC record has the form:
        // WARC header
        // \r\n\r\n
        // HTTP header
        // \r\n\r\n
        // payload
        // \r\n\r\n
        assertTrue("WARC record: incorrect format of WARC header",
                warcString.startsWith("WARC/1.0"));
        assertTrue("WARC record: incorrect format of HTTP header",
                warcString.contains("\r\n\r\nHTTP/1.1 200 OK\r\n"));
        assertTrue("WARC record: record is required to end with \\r\\n\\r\\n",
                warcString.endsWith("\r\n\r\n"));
        assertTrue("WARC record: payload mangled",
                warcString.endsWith("\r\n\r\nabcdef\r\n\r\n"));
        assertTrue(
                "WARC record: no or incorrect payload digest",
                warcString.contains("\r\nWARC-Payload-Digest: " + sha1str
                        + "\r\n"));
    }

}
