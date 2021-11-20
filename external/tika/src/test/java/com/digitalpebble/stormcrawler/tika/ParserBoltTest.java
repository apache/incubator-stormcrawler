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
package com.digitalpebble.stormcrawler.tika;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.digitalpebble.stormcrawler.Constants;
import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.TestUtil;
import com.digitalpebble.stormcrawler.parse.ParsingTester;
import com.digitalpebble.stormcrawler.persistence.Status;
import com.digitalpebble.stormcrawler.protocol.HttpHeaders;
import com.digitalpebble.stormcrawler.protocol.ProtocolResponse;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
     */
    public void testRecursiveDoc() throws IOException {

        Map conf = new HashMap();

        conf.put("parser.extract.embedded", true);

        bolt.prepare(conf, TestUtil.getMockedTopologyContext(), new OutputCollector(output));

        parse(
                "http://www.digitalpebble.com/test_recursive_embedded.docx",
                "test_recursive_embedded.docx");

        List<List<Object>> outTuples = output.getEmitted();

        // TODO could we get as many subdocs as embedded in the original one?
        // or just one for now? but should at least contain the text of the
        // subdocs

        Assert.assertEquals(1, outTuples.size());
        Assert.assertTrue(
                outTuples
                        .get(0)
                        .get(3)
                        .toString()
                        .contains("Life, Liberty and the pursuit of Happiness"));
    }

    @Test
    /**
     * Checks that the mimetype whitelists are handled correctly
     *
     * @see https://github.com/DigitalPebble/storm-crawler/issues/712
     */
    public void testMimeTypeWhileList() throws IOException {

        Map conf = new HashMap();

        conf.put("parser.mimetype.whitelist", "application/.+word.*");
        conf.put(ProtocolResponse.PROTOCOL_MD_PREFIX_PARAM, "http.");

        bolt.prepare(conf, TestUtil.getMockedTopologyContext(), new OutputCollector(output));

        String url = "http://thisisatest.com/adoc.pdf";
        Metadata metadata = new Metadata();
        metadata.addValue("http." + HttpHeaders.CONTENT_TYPE, "application/pdf");

        byte[] content = new byte[] {};
        Tuple tuple = mock(Tuple.class);
        when(tuple.getBinaryByField("content")).thenReturn(content);
        when(tuple.getStringByField("url")).thenReturn(url);
        when(tuple.getValueByField("metadata")).thenReturn(metadata);
        bolt.execute(tuple);

        List<List<Object>> outTuples = output.getEmitted(Constants.StatusStreamName);

        Assert.assertEquals(1, outTuples.size());
        Assert.assertTrue(outTuples.get(0).get(2).equals(Status.ERROR));

        outTuples.clear();

        metadata = new Metadata();
        metadata.addValue(
                "http." + HttpHeaders.CONTENT_TYPE,
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document");

        parse(
                "http://www.digitalpebble.com/test_recursive_embedded.docx",
                "test_recursive_embedded.docx",
                metadata);

        outTuples = output.getEmitted();

        Assert.assertEquals(1, outTuples.size());
    }
}
