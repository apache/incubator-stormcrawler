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

package com.digitalpebble.stormcrawler.bolt;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.storm.task.OutputCollector;

import com.digitalpebble.stormcrawler.Constants;
import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.TestUtil;
import com.digitalpebble.stormcrawler.bolt.SiteMapParserBolt;
import com.digitalpebble.stormcrawler.parse.ParsingTester;
import com.digitalpebble.stormcrawler.protocol.HttpHeaders;

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

        parse("http://www.digitalpebble.com/sitemap.xml",
                "digitalpebble.sitemap.xml", metadata);

        Assert.assertEquals(6, output.getEmitted(Constants.StatusStreamName)
                .size());
        // TODO test that the new links have the right metadata
        List<Object> fields = output.getEmitted(Constants.StatusStreamName)
                .get(0);
        Assert.assertEquals(3, fields.size());
    }

    @Test
    public void testSitemapParsingNoMT() throws IOException {

        Map parserConfig = new HashMap();
        parserConfig.put("sitemap.sniffContent", true);
        parserConfig.put("parsefilters.config.file", "test.parsefilters.json");
        bolt.prepare(parserConfig, TestUtil.getMockedTopologyContext(),
                new OutputCollector(output));

        Metadata metadata = new Metadata();
        // do not specify that it is a sitemap file
        // do not set the mimetype

        parse("http://www.digitalpebble.com/sitemap.xml",
                "digitalpebble.sitemap.xml", metadata);

        Assert.assertEquals(6, output.getEmitted(Constants.StatusStreamName)
                .size());
        // TODO test that the new links have the right metadata
        List<Object> fields = output.getEmitted(Constants.StatusStreamName)
                .get(0);
        Assert.assertEquals(3, fields.size());
    }

    @Test
    public void testNonSitemapParsing() throws IOException {

        prepareParserBolt("test.parsefilters.json");
        // do not specify that it is a sitemap file
        parse("http://www.digitalpebble.com", "digitalpebble.com.html",
                new Metadata());

        Assert.assertEquals(1, output.getEmitted().size());
    }

}
