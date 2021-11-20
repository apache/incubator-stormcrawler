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
import com.digitalpebble.stormcrawler.filtering.host.HostURLFilter;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

/**
 * Utility class which encapsulates the filtering of URLs based on the hostname or domain of the
 * source URL.
 */
public class HostURLFilterTest {

    private HostURLFilter createFilter(boolean ignoreOutsideHost, boolean ignoreOutsideDomain) {
        HostURLFilter filter = new HostURLFilter();
        ObjectNode filterParams = new ObjectNode(JsonNodeFactory.instance);
        filterParams.put("ignoreOutsideHost", Boolean.valueOf(ignoreOutsideHost));
        filterParams.put("ignoreOutsideDomain", Boolean.valueOf(ignoreOutsideDomain));
        Map<String, Object> conf = new HashMap<>();
        filter.configure(conf, filterParams);
        return filter;
    }

    @Test
    public void testAllAllowed() throws MalformedURLException {
        HostURLFilter allAllowed = createFilter(false, false);
        URL sourceURL = new URL("http://www.sourcedomain.com/index.html");
        Metadata metadata = new Metadata();
        String filterResult =
                allAllowed.filter(sourceURL, metadata, "http://www.sourcedomain.com/index.html");
        Assert.assertEquals("http://www.sourcedomain.com/index.html", filterResult);
        filterResult =
                allAllowed.filter(sourceURL, metadata, "http://www.anotherDomain.com/index.html");
        Assert.assertEquals("http://www.anotherDomain.com/index.html", filterResult);
        filterResult =
                allAllowed.filter(sourceURL, metadata, "http://sub.sourcedomain.com/index.html");
        Assert.assertEquals("http://sub.sourcedomain.com/index.html", filterResult);
    }

    @Test
    public void testAllForbidden() throws MalformedURLException {
        HostURLFilter allAllowed = createFilter(true, true);
        URL sourceURL = new URL("http://www.sourcedomain.com/index.html");
        Metadata metadata = new Metadata();

        String filterResult =
                allAllowed.filter(sourceURL, metadata, "http://www.sourcedomain.com/index.html");
        Assert.assertEquals("http://www.sourcedomain.com/index.html", filterResult);
        filterResult =
                allAllowed.filter(sourceURL, metadata, "http://www.anotherDomain.com/index.html");
        Assert.assertNull(filterResult);
        filterResult =
                allAllowed.filter(sourceURL, metadata, "http://sub.sourcedomain.com/index.html");
        Assert.assertNull(filterResult);
    }

    @Test
    public void testWithinHostOnly() throws MalformedURLException {
        HostURLFilter allAllowed = createFilter(true, false);
        URL sourceURL = new URL("http://www.sourcedomain.com/index.html");
        Metadata metadata = new Metadata();

        String filterResult =
                allAllowed.filter(sourceURL, metadata, "http://www.sourcedomain.com/index.html");
        Assert.assertEquals("http://www.sourcedomain.com/index.html", filterResult);
        filterResult =
                allAllowed.filter(sourceURL, metadata, "http://www.anotherDomain.com/index.html");
        Assert.assertNull(filterResult);
        filterResult =
                allAllowed.filter(sourceURL, metadata, "http://sub.sourcedomain.com/index.html");
        Assert.assertNull(filterResult);
    }

    @Test
    public void testWithinDomain() throws MalformedURLException {
        HostURLFilter allAllowed = createFilter(false, true);
        URL sourceURL = new URL("http://www.sourcedomain.com/index.html");
        Metadata metadata = new Metadata();

        String filterResult =
                allAllowed.filter(sourceURL, metadata, "http://www.sourcedomain.com/index.html");
        Assert.assertEquals("http://www.sourcedomain.com/index.html", filterResult);
        filterResult =
                allAllowed.filter(sourceURL, metadata, "http://www.anotherDomain.com/index.html");
        Assert.assertNull(filterResult);
        filterResult =
                allAllowed.filter(sourceURL, metadata, "http://sub.sourcedomain.com/index.html");
        Assert.assertEquals("http://sub.sourcedomain.com/index.html", filterResult);
    }
}
