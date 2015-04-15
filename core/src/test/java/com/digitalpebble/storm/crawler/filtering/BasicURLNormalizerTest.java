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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.digitalpebble.storm.crawler.Metadata;
import com.digitalpebble.storm.crawler.filtering.basic.BasicURLNormalizer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Utility class which encapsulates the filtering of URLs based on the hostname
 * or domain of the source URL.
 **/
public class BasicURLNormalizerTest {

    List<String> queryParamsToFilter = Arrays.asList("a", "foo");

    private URLFilter createFilter(boolean removeAnchor) {
        ObjectNode filterParams = new ObjectNode(JsonNodeFactory.instance);
        filterParams.put("removeAnchorPart", Boolean.valueOf(removeAnchor));
        return createFilter(filterParams);
    }

    private URLFilter createFilter(List<String> queryElementsToRemove) {
        ObjectNode filterParams = new ObjectNode(JsonNodeFactory.instance);
        filterParams.set("queryElementsToRemove", getArrayNode(queryElementsToRemove));
        return createFilter(filterParams);
    }

    private URLFilter createFilter(boolean removeAnchor, List<String> queryElementsToRemove) {
        ObjectNode filterParams = new ObjectNode(JsonNodeFactory.instance);
        filterParams.set("queryElementsToRemove", getArrayNode(queryElementsToRemove));
        filterParams.put("removeAnchorPart", Boolean.valueOf(removeAnchor));
        return createFilter(filterParams);
    }

    private URLFilter createFilter(boolean removeAnchor, boolean unmangleQueryString,
            List<String> queryElementsToRemove) {
        ObjectNode filterParams = new ObjectNode(JsonNodeFactory.instance);
        filterParams.set("queryElementsToRemove", getArrayNode(queryElementsToRemove));
        filterParams.put("removeAnchorPart", Boolean.valueOf(removeAnchor));
        filterParams.put("unmangleQueryString", Boolean.valueOf(unmangleQueryString));
        return createFilter(filterParams);
    }

    private URLFilter createFilter(ObjectNode filterParams) {
        BasicURLNormalizer filter = new BasicURLNormalizer();
        Map<String, Object> conf = new HashMap<String, Object>();
        filter.configure(conf, filterParams);
        return filter;
    }

    @Test
    public void testAnchorFilter() throws MalformedURLException {
        URLFilter allAllowed = createFilter(true);
        URL url = new URL("http://www.sourcedomain.com/#0");
        Metadata metadata = new Metadata();
        String filterResult = allAllowed.filter(url, metadata,
                url.toExternalForm());
        String expected = "http://www.sourcedomain.com/";
        Assert.assertEquals(expected, filterResult);
    }

    @Test
    public void testAnchorFilterFalse() throws MalformedURLException {
        URLFilter allAllowed = createFilter(false);
        URL url = new URL("http://www.sourcedomain.com/#0");
        Metadata metadata = new Metadata();
        String filterResult = allAllowed.filter(url, metadata, url.toExternalForm());
        Assert.assertEquals(url.toExternalForm(), filterResult);
    }

    @Test
    public void testRemoveSomeOfManyQueryParams() throws MalformedURLException {
        URLFilter urlFilter = createFilter(queryParamsToFilter);
        URL testSourceUrl = new URL("http://google.com");
        String testUrl = "http://google.com?keep1=true&a=c&foo=baz&keep2=true";
        String expectedResult = "http://google.com?keep1=true&keep2=true";
        String normalizedUrl = urlFilter.filter(testSourceUrl, new Metadata(), testUrl);
        assertEquals("Failed to filter query string", expectedResult, normalizedUrl);
    }

    @Test
    public void testRemoveAllQueryParams() throws MalformedURLException {
        URLFilter urlFilter = createFilter(queryParamsToFilter);
        URL testSourceUrl = new URL("http://google.com");
        String testUrl = "http://google.com?a=c&foo=baz";
        String expectedResult = "http://google.com";
        String normalizedUrl = urlFilter.filter(testSourceUrl, new Metadata(), testUrl);
        assertEquals("Failed to filter query string", expectedResult, normalizedUrl);
    }

    @Test
    public void testRemoveDupeQueryParams() throws MalformedURLException {
        URLFilter urlFilter = createFilter(queryParamsToFilter);
        URL testSourceUrl = new URL("http://google.com");
        String testUrl = "http://google.com?a=c&foo=baz&foo=bar&test=true";
        String expectedResult = "http://google.com?test=true";
        String normalizedUrl = urlFilter.filter(testSourceUrl, new Metadata(), testUrl);
        assertEquals("Failed to filter query string", expectedResult, normalizedUrl);
    }

    @Test
    public void testPipeInUrlAndFilterStillWorks() throws MalformedURLException {
        URLFilter urlFilter = createFilter(queryParamsToFilter);
        URL testSourceUrl = new URL("http://google.com");
        String testUrl = "http://google.com?a=c|d&foo=baz&foo=bar&test=true";
        String expectedResult = "http://google.com?test=true";
        String normalizedUrl = urlFilter.filter(testSourceUrl, new Metadata(), testUrl);
        assertEquals("Failed to filter query string", expectedResult, normalizedUrl);
    }

    @Test
    public void testBothAnchorAndQueryFilter() throws MalformedURLException {
        URLFilter urlFilter = createFilter(true, queryParamsToFilter);
        URL testSourceUrl = new URL("http://google.com");
        String testUrl = "http://google.com?a=c|d&foo=baz&foo=bar&test=true#fragment=ohYeah";
        String expectedResult = "http://google.com?test=true";
        String normalizedUrl = urlFilter.filter(testSourceUrl, new Metadata(), testUrl);
        assertEquals("Failed to filter query string", expectedResult, normalizedUrl);
    }

    @Test
    public void testQuerySort() throws MalformedURLException {
        URLFilter urlFilter = createFilter(queryParamsToFilter);
        URL testSourceUrl = new URL("http://google.com");
        String testUrl = "http://google.com?a=c|d&foo=baz&foo=bar&test=true&z=2&d=4";
        String expectedResult = "http://google.com?d=4&test=true&z=2";
        String normalizedUrl = urlFilter.filter(testSourceUrl, new Metadata(), testUrl);
        assertEquals("Failed to filter query string", expectedResult, normalizedUrl);
    }

    @Test
    public void testMangledQueryString() throws MalformedURLException {
        URLFilter urlFilter = createFilter(queryParamsToFilter);
        URL testSourceUrl = new URL("http://google.com");
        String testUrl = "http://google.com&d=4&good=true";
        String expectedResult = "http://google.com?d=4&good=true";
        String normalizedUrl = urlFilter.filter(testSourceUrl, new Metadata(), testUrl);
        assertEquals("Failed to filter query string", expectedResult, normalizedUrl);
    }

    @Test
    public void testDontFixMangledQueryString() throws MalformedURLException {
        URLFilter urlFilter = createFilter(true, false, queryParamsToFilter);
        URL testSourceUrl = new URL("http://google.com");
        String testUrl = "http://google.com&d=4&good=true";
        String expectedResult = "http://google.com&d=4&good=true";
        String normalizedUrl = urlFilter.filter(testSourceUrl, new Metadata(), testUrl);
        assertEquals("Failed to filter query string", expectedResult, normalizedUrl);
    }

    @Test
    public void testProperURLEncodingWithoutQueryParameter()
            throws MalformedURLException {
        URLFilter urlFilter = createFilter(queryParamsToFilter);
        String urlWithEscapedCharacters = "http://www.dillards.com/product/ASICS-Womens-GT2000-3-LiteShow%E2%84%A2-Running-Shoes_301_-1_301_504736989";
        URL testSourceUrl = new URL(urlWithEscapedCharacters);
        String testUrl = urlWithEscapedCharacters;
        String expectedResult = urlWithEscapedCharacters;
        String normalizedUrl = urlFilter.filter(testSourceUrl, new Metadata(),
                testUrl);
        assertEquals("Failed to filter query string", expectedResult,
                normalizedUrl);
    }

    @Test
    public void testProperURLEncodingWithQueryParameters()
            throws MalformedURLException {
        URLFilter urlFilter = createFilter(queryParamsToFilter);
        String urlWithEscapedCharacters = "http://www.dillards.com/product/ASICS-Womens-GT2000-3-LiteShow%E2%84%A2-Running-Shoes_301_-1_301_504736989?how=are&you=doing";
        URL testSourceUrl = new URL(urlWithEscapedCharacters);
        String testUrl = urlWithEscapedCharacters;
        String expectedResult = urlWithEscapedCharacters;
        String normalizedUrl = urlFilter.filter(testSourceUrl, new Metadata(),
                testUrl);
        assertEquals("Failed to filter query string", expectedResult,
                normalizedUrl);
    }

    private JsonNode getArrayNode(List<String> queryElementsToRemove) {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.valueToTree(queryElementsToRemove);
    }
}
