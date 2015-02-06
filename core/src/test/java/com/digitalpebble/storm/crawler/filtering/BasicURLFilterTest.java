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

import com.digitalpebble.storm.crawler.Metadata;
import com.digitalpebble.storm.crawler.filtering.basic.BasicURLFilter;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Utility class which encapsulates the filtering of URLs based on the hostname
 * or domain of the source URL.
 **/
public class BasicURLFilterTest {

    private URLFilter createFilter(boolean removeAnchor) {
        BasicURLFilter filter = new BasicURLFilter();
        ObjectNode filterParams = new ObjectNode(JsonNodeFactory.instance);
        filterParams.put("removeAnchorPart", Boolean.valueOf(removeAnchor));
        Map<String, Object> conf = new HashMap<String, Object>();
        filter.configure(conf, filterParams);
        return filter;
    }

    @Test
    public void testFilter() throws MalformedURLException {
        URLFilter allAllowed = createFilter(true);
        URL url = new URL("http://www.sourcedomain.com/#0");
        Metadata metadata = new Metadata();
        String filterResult = allAllowed.filter(url, metadata,
                url.toExternalForm());
        String expected = "http://www.sourcedomain.com/";
        Assert.assertEquals(expected, filterResult);
    }

}
