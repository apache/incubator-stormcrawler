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
import com.digitalpebble.stormcrawler.filtering.regex.FastURLFilter;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

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
        String filterResult = createFilter().filter(url, metadata, url.toExternalForm());
        Assert.assertEquals(null, filterResult);
    }

    @Test
    public void testDomainNotAllowed() throws MalformedURLException {
        URL url = new URL("http://stormcrawler.net/");
        Metadata metadata = new Metadata();
        String filterResult = createFilter().filter(url, metadata, url.toExternalForm());
        Assert.assertEquals(null, filterResult);

        // allowed
        url = new URL("http://stormcrawler.net/digitalpebble/");
        filterResult = createFilter().filter(url, metadata, url.toExternalForm());
        Assert.assertEquals(url.toString(), filterResult);
    }

    @Test
    public void testMD() throws MalformedURLException {
        URL url = new URL("http://somedomain.net/");
        Metadata metadata = new Metadata();
        metadata.addValue("key", "value");
        String filterResult = createFilter().filter(url, metadata, url.toExternalForm());
        Assert.assertEquals(null, filterResult);
    }
}
