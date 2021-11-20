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
package com.digitalpebble.stormcrawler.bolt;

import com.digitalpebble.stormcrawler.Constants;
import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.TestUtil;
import com.digitalpebble.stormcrawler.parse.ParsingTester;
import com.digitalpebble.stormcrawler.protocol.HttpHeaders;
import crawlercommons.sitemaps.extension.Extension;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Values;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SiteMapParserBoltTest extends ParsingTester {

    @Before
    public void setupParserBolt() {
        bolt = new SiteMapParserBolt();
        setupParserBolt(bolt);
    }

    // TODO add a test for a sitemap containing links
    // to other sitemap files

    @Test
    public void testSitemapParsing() throws IOException {

        prepareParserBolt("test.parsefilters.json");

        Metadata metadata = new Metadata();
        // specify that it is a sitemap file
        metadata.setValue(SiteMapParserBolt.isSitemapKey, "true");
        // and its mime-type
        metadata.setValue(HttpHeaders.CONTENT_TYPE, "application/xml");

        parse("http://www.digitalpebble.com/sitemap.xml", "digitalpebble.sitemap.xml", metadata);

        Assert.assertEquals(6, output.getEmitted(Constants.StatusStreamName).size());
        // TODO test that the new links have the right metadata
        List<Object> fields = output.getEmitted(Constants.StatusStreamName).get(0);
        Assert.assertEquals(3, fields.size());
    }

    @Test
    public void testSitemapParsingWithImageExtensions() throws IOException {
        Map parserConfig = new HashMap();
        parserConfig.put("sitemap.extensions", Collections.singletonList(Extension.IMAGE.name()));
        prepareParserBolt("test.parsefilters.json", parserConfig);

        Metadata metadata = new Metadata();
        // specify that it is a sitemap file
        metadata.setValue(SiteMapParserBolt.isSitemapKey, "true");
        // and its mime-type
        metadata.setValue(HttpHeaders.CONTENT_TYPE, "application/xml");

        parse(
                "http://www.digitalpebble.com/sitemap.xml",
                "digitalpebble.sitemap.extensions.image.xml",
                metadata);
        Values values = (Values) output.getEmitted(Constants.StatusStreamName).get(0);
        Metadata parsedMetadata = (Metadata) values.get(1);
        assertImageAttributes(parsedMetadata);
    }

    @Test
    public void testSitemapParsingWithMobileExtensions() throws IOException {
        Map parserConfig = new HashMap();
        parserConfig.put("sitemap.extensions", Collections.singletonList(Extension.MOBILE.name()));
        prepareParserBolt("test.parsefilters.json", parserConfig);

        Metadata metadata = new Metadata();
        // specify that it is a sitemap file
        metadata.setValue(SiteMapParserBolt.isSitemapKey, "true");
        // and its mime-type
        metadata.setValue(HttpHeaders.CONTENT_TYPE, "application/xml");

        parse(
                "http://www.digitalpebble.com/sitemap.xml",
                "digitalpebble.sitemap.extensions.mobile.xml",
                metadata);
        Values values = (Values) output.getEmitted(Constants.StatusStreamName).get(0);
        Metadata parsedMetadata = (Metadata) values.get(1);
        assertMobileAttributes(parsedMetadata);
    }

    @Test
    public void testSitemapParsingWithLinkExtensions() throws IOException {
        Map parserConfig = new HashMap();
        parserConfig.put("sitemap.extensions", Collections.singletonList(Extension.LINKS.name()));
        prepareParserBolt("test.parsefilters.json", parserConfig);

        Metadata metadata = new Metadata();
        // specify that it is a sitemap file
        metadata.setValue(SiteMapParserBolt.isSitemapKey, "true");
        // and its mime-type
        metadata.setValue(HttpHeaders.CONTENT_TYPE, "application/xml");

        parse(
                "http://www.digitalpebble.com/sitemap.xml",
                "digitalpebble.sitemap.extensions.links.xml",
                metadata);
        Values values = (Values) output.getEmitted(Constants.StatusStreamName).get(0);
        Metadata parsedMetadata = (Metadata) values.get(1);
        assertLinksAttributes(parsedMetadata);
    }

    @Test
    public void testSitemapParsingWithNewsExtensions() throws IOException {
        Map parserConfig = new HashMap();
        parserConfig.put("sitemap.extensions", Collections.singletonList(Extension.NEWS.name()));
        prepareParserBolt("test.parsefilters.json", parserConfig);

        Metadata metadata = new Metadata();
        // specify that it is a sitemap file
        metadata.setValue(SiteMapParserBolt.isSitemapKey, "true");
        // and its mime-type
        metadata.setValue(HttpHeaders.CONTENT_TYPE, "application/xml");

        parse(
                "http://www.digitalpebble.com/sitemap.xml",
                "digitalpebble.sitemap.extensions.news.xml",
                metadata);
        Values values = (Values) output.getEmitted(Constants.StatusStreamName).get(0);
        Metadata parsedMetadata = (Metadata) values.get(1);
        assertNewsAttributes(parsedMetadata);
    }

    @Test
    public void testSitemapParsingWithVideoExtensions() throws IOException {
        Map parserConfig = new HashMap();
        parserConfig.put("sitemap.extensions", Collections.singletonList(Extension.VIDEO.name()));
        prepareParserBolt("test.parsefilters.json", parserConfig);

        Metadata metadata = new Metadata();
        // specify that it is a sitemap file
        metadata.setValue(SiteMapParserBolt.isSitemapKey, "true");
        // and its mime-type
        metadata.setValue(HttpHeaders.CONTENT_TYPE, "application/xml");

        parse(
                "http://www.digitalpebble.com/sitemap.xml",
                "digitalpebble.sitemap.extensions.video.xml",
                metadata);
        Values values = (Values) output.getEmitted(Constants.StatusStreamName).get(0);
        Metadata parsedMetadata = (Metadata) values.get(1);
        assertVideoAttributes(parsedMetadata);
    }

    @Test
    public void testSitemapParsingWithAllExtensions() throws IOException {
        Map parserConfig = new HashMap();

        parserConfig.put(
                "sitemap.extensions",
                Arrays.asList(
                        Extension.MOBILE.name(),
                        Extension.NEWS.name(),
                        Extension.LINKS.name(),
                        Extension.VIDEO.name(),
                        Extension.IMAGE.name()));
        prepareParserBolt("test.parsefilters.json", parserConfig);

        Metadata metadata = new Metadata();
        // specify that it is a sitemap file
        metadata.setValue(SiteMapParserBolt.isSitemapKey, "true");
        // and its mime-type
        metadata.setValue(HttpHeaders.CONTENT_TYPE, "application/xml");

        parse(
                "http://www.digitalpebble.com/sitemap.xml",
                "digitalpebble.sitemap.extensions.all.xml",
                metadata);
        Values values = (Values) output.getEmitted(Constants.StatusStreamName).get(0);
        Metadata parsedMetadata = (Metadata) values.get(1);
        assertImageAttributes(parsedMetadata);
        assertNewsAttributes(parsedMetadata);
        assertVideoAttributes(parsedMetadata);
        assertLinksAttributes(parsedMetadata);
        assertMobileAttributes(parsedMetadata);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSitemapParsingWithIllegalExtensionConfigured() throws IOException {
        Map parserConfig = new HashMap();
        parserConfig.put("sitemap.extensions", Arrays.asList("AUDIONEWSLINKS"));
        prepareParserBolt("test.parsefilters.json", parserConfig);
    }

    @Test
    public void testSitemapParsingNoMT() throws IOException {

        Map parserConfig = new HashMap();
        parserConfig.put("sitemap.sniffContent", true);
        parserConfig.put("parsefilters.config.file", "test.parsefilters.json");
        bolt.prepare(
                parserConfig, TestUtil.getMockedTopologyContext(), new OutputCollector(output));

        Metadata metadata = new Metadata();
        // do not specify that it is a sitemap file
        // do not set the mimetype

        parse("http://www.digitalpebble.com/sitemap.xml", "digitalpebble.sitemap.xml", metadata);

        Assert.assertEquals(6, output.getEmitted(Constants.StatusStreamName).size());
        // TODO test that the new links have the right metadata
        List<Object> fields = output.getEmitted(Constants.StatusStreamName).get(0);
        Assert.assertEquals(3, fields.size());
    }

    @Test
    public void testNonSitemapParsing() throws IOException {

        prepareParserBolt("test.parsefilters.json");
        // do not specify that it is a sitemap file
        parse("http://www.digitalpebble.com", "digitalpebble.com.html", new Metadata());

        Assert.assertEquals(1, output.getEmitted().size());
    }

    private void assertNewsAttributes(Metadata metadata) {
        long numAttributes =
                metadata.keySet().stream()
                        .filter(key -> key.startsWith(Extension.NEWS.name() + "."))
                        .count();
        Assert.assertEquals(7, numAttributes);
        Assert.assertEquals(
                "The Example Times", metadata.getFirstValue(Extension.NEWS.name() + "." + "name"));
        Assert.assertEquals("en", metadata.getFirstValue(Extension.NEWS.name() + "." + "language"));
        Assert.assertArrayEquals(
                new String[] {"PressRelease", "Blog"},
                metadata.getValues(Extension.NEWS.name() + "." + "genres"));
        Assert.assertEquals(
                "2008-12-23T00:00Z",
                metadata.getFirstValue(Extension.NEWS.name() + "." + "publication_date"));
        Assert.assertEquals(
                "Companies A, B in Merger Talks",
                metadata.getFirstValue(Extension.NEWS.name() + "." + "title"));
        Assert.assertArrayEquals(
                new String[] {"business", "merger", "acquisition", "A", "B"},
                metadata.getValues(Extension.NEWS.name() + "." + "keywords"));
        Assert.assertArrayEquals(
                new String[] {"NASDAQ:A", "NASDAQ:B"},
                metadata.getValues(Extension.NEWS.name() + "." + "stock_tickers"));
    }

    private void assertImageAttributes(Metadata metadata) {
        long numAttributes =
                metadata.keySet().stream()
                        .filter(key -> key.startsWith(Extension.IMAGE.name() + "."))
                        .count();
        Assert.assertEquals(5, numAttributes);
        Assert.assertEquals(
                "This is the caption.",
                metadata.getFirstValue(Extension.IMAGE.name() + "." + "caption"));
        Assert.assertEquals(
                "http://example.com/photo.jpg",
                metadata.getFirstValue(Extension.IMAGE.name() + "." + "loc"));
        Assert.assertEquals(
                "Example photo shot in Limerick, Ireland",
                metadata.getFirstValue(Extension.IMAGE.name() + "." + "title"));
        Assert.assertEquals(
                "https://creativecommons.org/licenses/by/4.0/legalcode",
                metadata.getFirstValue(Extension.IMAGE.name() + "." + "license"));
        Assert.assertEquals(
                "Limerick, Ireland",
                metadata.getFirstValue(Extension.IMAGE.name() + "." + "geo_location"));
    }

    private void assertLinksAttributes(Metadata metadata) {
        long numAttributes =
                metadata.keySet().stream()
                        .filter(key -> key.startsWith(Extension.LINKS.name() + "."))
                        .count();
        Assert.assertEquals(3, numAttributes);
        Assert.assertEquals(
                "alternate", metadata.getFirstValue(Extension.LINKS.name() + "." + "params.rel"));
        Assert.assertEquals(
                "http://www.example.com/english/",
                metadata.getFirstValue(Extension.LINKS.name() + "." + "href"));
        Assert.assertEquals(
                "en", metadata.getFirstValue(Extension.LINKS.name() + "." + "params.hreflang"));
    }

    private void assertVideoAttributes(Metadata metadata) {
        long numAttributes =
                metadata.keySet().stream()
                        .filter(key -> key.startsWith(Extension.VIDEO.name() + "."))
                        .count();
        Assert.assertEquals(20, numAttributes);
        Assert.assertEquals(
                "http://www.example.com/thumbs/123.jpg",
                metadata.getFirstValue(Extension.VIDEO.name() + "." + "thumbnail_loc"));
        Assert.assertEquals(
                "Grilling steaks for summer",
                metadata.getFirstValue(Extension.VIDEO.name() + "." + "title"));
        Assert.assertEquals(
                "Alkis shows you how to get perfectly done steaks every time",
                metadata.getFirstValue(Extension.VIDEO.name() + "." + "description"));
        Assert.assertEquals(
                "http://www.example.com/video123.flv",
                metadata.getFirstValue(Extension.VIDEO.name() + "." + "content_loc"));
        Assert.assertEquals(
                "http://www.example.com/videoplayer.swf?video=123",
                metadata.getFirstValue(Extension.VIDEO.name() + "." + "player_loc"));
        // Assert.assertEquals("600", metadata.getFirstValue(Extension.VIDEO.name() +
        // "." + "duration"));
        Assert.assertEquals(
                "2009-11-05T19:20:30+08:00",
                metadata.getFirstValue(Extension.VIDEO.name() + "." + "expiration_date"));
        Assert.assertEquals("4.2", metadata.getFirstValue(Extension.VIDEO.name() + "." + "rating"));
        Assert.assertEquals(
                "12345", metadata.getFirstValue(Extension.VIDEO.name() + "." + "view_count"));
        Assert.assertEquals(
                "2007-11-05T19:20:30+08:00",
                metadata.getFirstValue(Extension.VIDEO.name() + "." + "publication_date"));
        Assert.assertEquals(
                "true", metadata.getFirstValue(Extension.VIDEO.name() + "." + "family_friendly"));
        Assert.assertArrayEquals(
                new String[] {"sample_tag1", "sample_tag2"},
                metadata.getValues(Extension.VIDEO.name() + "." + "tags"));
        Assert.assertArrayEquals(
                new String[] {"IE", "GB", "US", "CA"},
                metadata.getValues(Extension.VIDEO.name() + "." + "allowed_countries"));
        Assert.assertEquals(
                "http://cooking.example.com",
                metadata.getFirstValue(Extension.VIDEO.name() + "." + "gallery_loc"));
        Assert.assertEquals(
                "value: 1.99, currency: EUR, type: own, resolution: null",
                metadata.getFirstValue(Extension.VIDEO.name() + "." + "prices"));
        Assert.assertEquals(
                "true",
                metadata.getFirstValue(Extension.VIDEO.name() + "." + "requires_subscription"));
        Assert.assertEquals(
                "GrillyMcGrillerson",
                metadata.getFirstValue(Extension.VIDEO.name() + "." + "uploader"));
        Assert.assertEquals(
                "http://www.example.com/users/grillymcgrillerson",
                metadata.getFirstValue(Extension.VIDEO.name() + "." + "uploader_info"));
        Assert.assertEquals(
                "false", metadata.getFirstValue(Extension.VIDEO.name() + "." + "is_live"));
    }

    private void assertMobileAttributes(Metadata metadata) {
        long numAttributes =
                metadata.keySet().stream()
                        .filter(key -> key.startsWith(Extension.MOBILE.name() + "."))
                        .count();
        Assert.assertEquals(0, numAttributes);
    }
}
