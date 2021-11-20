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
import com.digitalpebble.stormcrawler.filtering.basic.BasicURLFilter;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class BasicURLFilterTest {

    private URLFilter createFilter(int length, int repet) {
        BasicURLFilter filter = new BasicURLFilter();
        ObjectNode filterParams = new ObjectNode(JsonNodeFactory.instance);
        filterParams.put("maxPathRepetition", repet);
        filterParams.put("maxLength", length);
        Map<String, Object> conf = new HashMap<>();
        filter.configure(conf, filterParams);
        return filter;
    }

    @Test
    public void testRepetition() throws MalformedURLException {
        URLFilter filter = createFilter(-1, 3);
        Metadata metadata = new Metadata();

        URL targetURL = new URL("http://www.sourcedomain.com/a/a/a/index.html");
        String filterResult = filter.filter(targetURL, metadata, targetURL.toExternalForm());
        Assert.assertEquals(null, filterResult);

        targetURL = new URL("http://www.sourcedomain.com/a/b/a/index.html");
        filterResult = filter.filter(targetURL, metadata, targetURL.toExternalForm());
        Assert.assertEquals(targetURL.toExternalForm(), filterResult);
    }

    @Test
    public void testLength() throws MalformedURLException {
        URLFilter filter = createFilter(32, -1);
        Metadata metadata = new Metadata();

        URL targetURL = new URL("http://www.sourcedomain.com/a/a/a/index.html");
        String filterResult = filter.filter(targetURL, metadata, targetURL.toExternalForm());
        Assert.assertEquals(null, filterResult);

        targetURL = new URL("http://www.sourcedomain.com/");
        filterResult = filter.filter(targetURL, metadata, targetURL.toExternalForm());
        Assert.assertEquals(targetURL.toExternalForm(), filterResult);
    }
}
