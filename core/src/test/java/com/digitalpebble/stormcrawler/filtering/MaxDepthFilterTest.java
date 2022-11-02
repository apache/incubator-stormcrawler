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
package com.digitalpebble.stormcrawler.filtering;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.filtering.depth.MaxDepthFilter;
import com.digitalpebble.stormcrawler.util.MetadataTransfer;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class MaxDepthFilterTest {

    private URLFilter createFilter(String key, int value) {
        MaxDepthFilter filter = new MaxDepthFilter();
        ObjectNode filterParams = new ObjectNode(JsonNodeFactory.instance);
        filterParams.put(key, value);
        Map<String, Object> conf = new HashMap<>();
        filter.configure(conf, filterParams);
        return filter;
    }

    @Test
    public void testDepthZero() throws MalformedURLException {
        URLFilter filter = createFilter("maxDepth", 0);
        URL url = new URL("http://www.sourcedomain.com/");
        Metadata metadata = new Metadata();
        String filterResult = filter.filter(url, metadata, url.toExternalForm());
        Assert.assertEquals(null, filterResult);
    }

    @Test
    public void testDepth() throws MalformedURLException {
        URLFilter filter = createFilter("maxDepth", 2);
        URL url = new URL("http://www.sourcedomain.com/");
        Metadata metadata = new Metadata();
        metadata.setValue(MetadataTransfer.depthKeyName, "2");
        String filterResult = filter.filter(url, metadata, url.toExternalForm());
        Assert.assertEquals(null, filterResult);
    }

    @Test
    public void testCustomDepthZero() throws MalformedURLException {
        URLFilter filter = createFilter("maxDepth", 3);
        URL url = new URL("http://www.sourcedomain.com/");
        Metadata metadata = new Metadata();
        metadata.setValue(MetadataTransfer.maxDepthKeyName, "0");
        String filterResult = filter.filter(url, metadata, url.toExternalForm());
        Assert.assertEquals(null, filterResult);
    }

    @Test
    public void testCustomDepth() throws MalformedURLException {
        URLFilter filter = createFilter("maxDepth", 1);
        URL url = new URL("http://www.sourcedomain.com/");
        Metadata metadata = new Metadata();
        metadata.setValue(MetadataTransfer.maxDepthKeyName, "2");
        metadata.setValue(MetadataTransfer.depthKeyName, "1");
        String filterResult = filter.filter(url, metadata, url.toExternalForm());
        Assert.assertEquals(url.toExternalForm(), filterResult);
    }
}
