package com.digitalpebble.stormcrawler;

import org.junit.Assert;
import org.junit.Test;

public class MetadataTest {

    @Test
    public void testCopyWithPrefix() {
        Metadata metadata = new Metadata();
        metadata.addValue("fetch.statusCode", "500");
        metadata.addValue("fetch.error.count", "2");
        metadata.addValue("fetch.exception", "java.lang.Exception");
        metadata.addValue("fetchInterval", "200");
        metadata.addValue("isFeed", "true");
        metadata.addValue("depth", "1");

        Metadata copy = new Metadata();
        for (String key : metadata.keySet("fetch.")) {
            metadata.copy(copy, key);
        }

        Assert.assertEquals(3, copy.size());
    }
}
