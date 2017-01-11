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

package com.digitalpebble.stormcrawler.tika;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.digitalpebble.stormcrawler.TestUtil;
import com.digitalpebble.stormcrawler.parse.ParsingTester;

public class ParserBoltTest extends ParsingTester {

    @Before
    public void setupParserBolt() {
        bolt = new ParserBolt();
        setupParserBolt(bolt);
    }

    @Test
    /**
     * Checks that recursive docs are handled correctly
     * 
     * @see https://issues.apache.org/jira/browse/TIKA-2096
     **/
    public void testRecursiveDoc() throws IOException {

        Map conf = new HashMap();

        conf.put("parser.extract.embedded", true);

        bolt.prepare(conf, TestUtil.getMockedTopologyContext(),
                new OutputCollector(output));

        parse("http://www.digitalpebble.com/test_recursive_embedded.docx",
                "test_recursive_embedded.docx");

        List<List<Object>> outTuples = output.getEmitted();

        // TODO could we get as many subdocs as embedded in the original one?
        // or just one for now? but should at least contain the text of the
        // subdocs

        Assert.assertEquals(1, outTuples.size());
        Assert.assertTrue(outTuples.get(0).get(3).toString()
                .contains("Life, Liberty and the pursuit of Happiness"));

    }

}
