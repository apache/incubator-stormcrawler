package com.digitalpebble.stormcrawler.filtering;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.filtering.regex.FastURLFilter;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class FastURLFilterTest {

    private URLFilter createFilter() {
        ObjectNode filterParams = new ObjectNode(JsonNodeFactory.instance);
        filterParams.put("files", "fast.urlfilter.json");
        FastURLFilter filter = new FastURLFilter();
        Map<String, Object> conf = new HashMap<>();
        filter.configure(conf, filterParams);
        return filter;
    }

    @Test
    public void testImagesFilter() throws MalformedURLException {
        URL url = new URL("http://www.somedomain.com/image.jpg");
        Metadata metadata = new Metadata();
        String filterResult = createFilter().filter(url, metadata,
                url.toExternalForm());
        Assert.assertEquals(null, filterResult);
    }

    @Test
    public void testDomainNotAllowed() throws MalformedURLException {
        URL url = new URL("http://stormcrawler.net/");
        Metadata metadata = new Metadata();
        String filterResult = createFilter().filter(url, metadata,
                url.toExternalForm());
        Assert.assertEquals(null, filterResult);
    }

    @Test
    public void testMD() throws MalformedURLException {
        URL url = new URL("http://somedomain.net/");
        Metadata metadata = new Metadata();
        metadata.addValue("key", "value");
        String filterResult = createFilter().filter(url, metadata,
                url.toExternalForm());
        Assert.assertEquals(null, filterResult);
    }

}
