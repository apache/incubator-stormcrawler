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

package com.digitalpebble.storm.crawler.bolt;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.digitalpebble.storm.crawler.parse.filter.ParsingTester;
import com.digitalpebble.storm.crawler.protocol.HttpHeaders;
import com.digitalpebble.storm.crawler.util.KeyValues;

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

        Map<String, String[]> metadata = KeyValues.newInstance();
        // specify that it is a sitemap file
        KeyValues.setValue(SiteMapParserBolt.isSitemapKey, metadata, "true");
        // and its mime-type
        KeyValues.setValue(HttpHeaders.CONTENT_TYPE, metadata,
                "application/xml");

        parse("http://www.digitalpebble.com/sitemap.xml",
                "digitalpebble.sitemap.xml", metadata);

        Assert.assertEquals(6, output.getEmitted().size());
        // TODO test that the new links have the right metadata
        List<Object> fields = output.getEmitted().get(0);
        Assert.assertEquals(3, fields.size());
    }

    @Test
    public void testNonSitemapParsing() throws IOException {

        prepareParserBolt("test.parsefilters.json");

        Map<String, String[]> metadata = KeyValues.newInstance();
        // do not specify that it is a sitemap file
        parse("http://www.digitalpebble.com", "digitalpebble.com.html",
                metadata);

        Assert.assertEquals(1, output.getEmitted().size());
    }

}
