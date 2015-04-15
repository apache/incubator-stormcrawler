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

package com.digitalpebble.storm.crawler.parse.filter;

import java.io.IOException;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.digitalpebble.storm.crawler.Metadata;
import com.digitalpebble.storm.crawler.bolt.JSoupParserBolt;

public class XPathFilterTest extends ParsingTester {

    @Before
    public void setupParserBolt() {
        bolt = new JSoupParserBolt();
        setupParserBolt(bolt);
    }

    @Test
    public void testBasicExtraction() throws IOException {

        prepareParserBolt("test.parsefilters.json");

        parse("http://www.digitalpebble.com", "digitalpebble.com.html");

        Assert.assertEquals(1, output.getEmitted().size());
        List<Object> parsedTuple = output.getEmitted().get(0);
        Metadata metadata = (Metadata) parsedTuple.get(2);
        Assert.assertNotNull(metadata);
        String concept = metadata.getFirstValue("concept");
        Assert.assertNotNull(concept);

        concept = metadata.getFirstValue("concept2");
        Assert.assertNotNull(concept);
    }

}
