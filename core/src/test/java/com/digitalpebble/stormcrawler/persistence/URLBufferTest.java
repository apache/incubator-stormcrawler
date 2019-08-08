package com.digitalpebble.stormcrawler.persistence;

import java.net.MalformedURLException;

import org.junit.Assert;
import org.junit.Test;

import com.digitalpebble.stormcrawler.Metadata;

public class URLBufferTest {
    @Test
    public void testURLBuffer() throws MalformedURLException {
        URLBuffer buffer = new URLBuffer();
        Assert.assertFalse(buffer.hasNext());
        buffer.add("http://a.net/test.html", new Metadata());
        buffer.add("http://b.net/test.html", new Metadata());
        buffer.add("http://c.net/test.html", new Metadata());
        Assert.assertEquals("http://a.net/test.html",buffer.next().get(0));
        Assert.assertEquals("http://b.net/test.html",buffer.next().get(0));
    }
}
