package com.digitalpebble.stormcrawler.warc;

import static org.junit.Assert.assertEquals;

import java.nio.charset.StandardCharsets;

import org.junit.Test;

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

}
