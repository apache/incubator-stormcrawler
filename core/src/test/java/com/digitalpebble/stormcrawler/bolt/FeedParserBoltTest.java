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
import com.digitalpebble.stormcrawler.protocol.ProtocolResponse;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FeedParserBoltTest extends ParsingTester {

    @Before
    public void setupParserBolt() {
        bolt = new FeedParserBolt();
        setupParserBolt(bolt);
    }

    private void checkOutput() {
        Assert.assertEquals(170, output.getEmitted(Constants.StatusStreamName).size());
        List<Object> fields = output.getEmitted(Constants.StatusStreamName).get(0);
        Assert.assertEquals(3, fields.size());
    }

    @Test
    public void testFeedParsing() throws IOException {

        prepareParserBolt("test.parsefilters.json");

        Metadata metadata = new Metadata();
        // specify that it is a Feed file
        metadata.setValue(FeedParserBolt.isFeedKey, "true");
        parse("http://www.guardian.com/Feed.xml", "guardian.rss", metadata);
        checkOutput();
    }

    @Test
    public void testFeedParsingNoMT() throws IOException {

        Map parserConfig = new HashMap();
        parserConfig.put("feed.sniffContent", true);
        parserConfig.put("parsefilters.config.file", "test.parsefilters.json");
        parserConfig.put(ProtocolResponse.PROTOCOL_MD_PREFIX_PARAM, "http.");
        bolt.prepare(
                parserConfig, TestUtil.getMockedTopologyContext(), new OutputCollector(output));

        Metadata metadata = new Metadata();

        // set mime-type
        metadata.setValue("http." + HttpHeaders.CONTENT_TYPE, "application/rss+xml");

        parse("http://www.guardian.com/feed.xml", "guardian.rss", metadata);

        checkOutput();
    }

    @Test
    public void testFeedParsingDetextBytes() throws IOException {

        Map parserConfig = new HashMap();
        parserConfig.put("feed.sniffContent", true);
        parserConfig.put("parsefilters.config.file", "test.parsefilters.json");
        bolt.prepare(
                parserConfig, TestUtil.getMockedTopologyContext(), new OutputCollector(output));

        Metadata metadata = new Metadata();
        parse("http://www.guardian.com/feed.xml", "guardian.rss", metadata);

        checkOutput();
    }

    @Test
    public void testNonFeedParsing() throws IOException {

        prepareParserBolt("test.parsefilters.json");
        // do not specify that it is a feed file
        parse("http://www.digitalpebble.com", "digitalpebble.com.html", new Metadata());

        Assert.assertEquals(1, output.getEmitted().size());
    }
}
