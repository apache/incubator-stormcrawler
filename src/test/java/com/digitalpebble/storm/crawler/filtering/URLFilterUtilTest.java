/**
 * Licensed to DigitalPebble Ltd under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * DigitalPebble licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.digitalpebble.storm.crawler.filtering;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

/**
 * Utility class which encapsulates the filtering of URLs based on the hostname
 * or domain of the source URL.
 **/
public class URLFilterUtilTest {

    @Test
    public void testAllAllowed() throws MalformedURLException {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put("parser.ignore.outlinks.outside.host", false);
        conf.put("parser.ignore.outlinks.outside.domain", false);
        URLFilterUtil allAllowed = new URLFilterUtil(conf);
        URL sourceURL = new URL("http://www.sourcedomain.com/index.html");
        allAllowed.setSourceURL(sourceURL);
        boolean kept = allAllowed.filter("http://www.sourcedomain.com/index.html");
        Assert.assertTrue(kept);
        kept = allAllowed.filter("http://www.anotherDomain.com/index.html");
        Assert.assertTrue(kept);
        kept = allAllowed.filter("http://sub.sourcedomain.com/index.html");
        Assert.assertTrue(kept);
    }
    
    @Test
    public void testAllForbidden() throws MalformedURLException {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put("parser.ignore.outlinks.outside.host", true);
        conf.put("parser.ignore.outlinks.outside.domain", true);
        URLFilterUtil allAllowed = new URLFilterUtil(conf);
        URL sourceURL = new URL("http://www.sourcedomain.com/index.html");
        allAllowed.setSourceURL(sourceURL);
        boolean kept = allAllowed.filter("http://www.sourcedomain.com/index.html");
        Assert.assertTrue(kept);
        kept = allAllowed.filter("http://www.anotherDomain.com/index.html");
        Assert.assertFalse(kept);
        kept = allAllowed.filter("http://sub.sourcedomain.com/index.html");
        Assert.assertFalse(kept);
    }

    @Test
    public void testWithinHostOnly() throws MalformedURLException {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put("parser.ignore.outlinks.outside.host", true);
        conf.put("parser.ignore.outlinks.outside.domain", false);
        URLFilterUtil allAllowed = new URLFilterUtil(conf);
        URL sourceURL = new URL("http://www.sourcedomain.com/index.html");
        allAllowed.setSourceURL(sourceURL);
        boolean kept = allAllowed.filter("http://www.sourcedomain.com/index.html");
        Assert.assertTrue(kept);
        kept = allAllowed.filter("http://www.anotherDomain.com/index.html");
        Assert.assertFalse(kept);
        kept = allAllowed.filter("http://sub.sourcedomain.com/index.html");
        Assert.assertFalse(kept);
    }
    
    public void testWithinDomain() throws MalformedURLException {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put("parser.ignore.outlinks.outside.host", false);
        conf.put("parser.ignore.outlinks.outside.domain", true);
        URLFilterUtil allAllowed = new URLFilterUtil(conf);
        URL sourceURL = new URL("http://www.sourcedomain.com/index.html");
        allAllowed.setSourceURL(sourceURL);
        boolean kept = allAllowed.filter("http://www.sourcedomain.com/index.html");
        Assert.assertFalse(true);
        kept = allAllowed.filter("http://www.anotherDomain.com/index.html");
        Assert.assertFalse(kept);
        kept = allAllowed.filter("http://sub.sourcedomain.com/index.html");
        Assert.assertTrue(kept);
    }
    
}
