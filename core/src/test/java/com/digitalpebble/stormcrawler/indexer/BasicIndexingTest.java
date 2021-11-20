/*
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

package com.digitalpebble.stormcrawler.indexer;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.indexing.AbstractIndexerBolt;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BasicIndexingTest extends IndexerTester {

    private static final String URL = "http://www.digitalpebble.com";

    @Before
    public void setupIndexerBolt() {
        bolt = new DummyIndexer();
        setupIndexerBolt(bolt);
    }

    @Test
    public void testEmptyCanonicalURL() throws Exception {
        Map config = new HashMap();
        config.put(AbstractIndexerBolt.urlFieldParamName, "url");
        config.put(AbstractIndexerBolt.canonicalMetadataParamName, "canonical");

        prepareIndexerBolt(config);

        index(URL, new Metadata());
        Map<String, String> fields = ((DummyIndexer) bolt).returnFields();

        Assert.assertEquals(
                "The URL should be used if no canonical URL is found", URL, fields.get("url"));
    }

    @Test
    public void testCanonicalURL() throws Exception {
        Map config = new HashMap();
        config.put(AbstractIndexerBolt.urlFieldParamName, "url");
        config.put(AbstractIndexerBolt.canonicalMetadataParamName, "canonical");

        Metadata metadata = new Metadata();
        metadata.setValue("canonical", "http://www.digitalpebble.com/");

        prepareIndexerBolt(config);

        index(URL, metadata);
        Map<String, String> fields = ((DummyIndexer) bolt).returnFields();

        Assert.assertEquals(
                "Use the canonical URL if found",
                "http://www.digitalpebble.com/",
                fields.get("url"));
    }

    @Test
    public void testRelativeCanonicalURL() throws Exception {
        Map config = new HashMap();
        config.put(AbstractIndexerBolt.urlFieldParamName, "url");
        config.put(AbstractIndexerBolt.canonicalMetadataParamName, "canonical");

        Metadata metadata = new Metadata();
        metadata.setValue("canonical", "/home");

        prepareIndexerBolt(config);

        index(URL, metadata);
        Map<String, String> fields = ((DummyIndexer) bolt).returnFields();

        Assert.assertEquals(
                "Use the canonical URL if found",
                "http://www.digitalpebble.com/home",
                fields.get("url"));
    }

    @Test
    public void testBadCanonicalURL() throws Exception {
        Map config = new HashMap();
        config.put(AbstractIndexerBolt.urlFieldParamName, "url");
        config.put(AbstractIndexerBolt.canonicalMetadataParamName, "canonical");

        Metadata metadata = new Metadata();
        metadata.setValue("canonical", "htp://www.digitalpebble.com/");

        prepareIndexerBolt(config);

        index(URL, metadata);
        Map<String, String> fields = ((DummyIndexer) bolt).returnFields();

        Assert.assertEquals(
                "Use the default URL if a bad canonical URL is found",
                "http://www.digitalpebble.com",
                fields.get("url"));
    }

    @Test
    public void testOtherHostCanonicalURL() throws Exception {
        Map config = new HashMap();
        config.put(AbstractIndexerBolt.urlFieldParamName, "url");
        config.put(AbstractIndexerBolt.canonicalMetadataParamName, "canonical");

        Metadata metadata = new Metadata();
        metadata.setValue("canonical", "http://www.google.com/");

        prepareIndexerBolt(config);

        index(URL, metadata);
        Map<String, String> fields = ((DummyIndexer) bolt).returnFields();

        Assert.assertEquals(
                "Ignore if the canonical URL references other host",
                "http://www.digitalpebble.com",
                fields.get("url"));
    }

    @Test
    public void testMissingCanonicalParamConfiguration() throws Exception {
        Map config = new HashMap();
        config.put(AbstractIndexerBolt.urlFieldParamName, "url");

        Metadata metadata = new Metadata();
        metadata.setValue("canonical", "http://www.digitalpebble.com/");

        prepareIndexerBolt(config);

        index(URL, metadata);
        Map<String, String> fields = ((DummyIndexer) bolt).returnFields();

        Assert.assertEquals(
                "Use the canonical URL if found",
                "http://www.digitalpebble.com",
                fields.get("url"));
    }

    @Test
    public void testFilterDocumentWithMetadata() throws Exception {
        Map config = new HashMap();
        config.put(AbstractIndexerBolt.urlFieldParamName, "url");
        config.put(AbstractIndexerBolt.metadataFilterParamName, "key1=value1");

        Metadata metadata = new Metadata();
        metadata.setValue("key1", "value1");

        prepareIndexerBolt(config);

        index(URL, metadata);
        Map<String, String> fields = ((DummyIndexer) bolt).returnFields();

        Assert.assertEquals(
                "The document must pass if the key/value is found in the metadata",
                "http://www.digitalpebble.com",
                fields.get("url"));
    }

    @Test
    public void testFilterDocumentWithoutMetadata() throws Exception {
        Map config = new HashMap();
        config.put(AbstractIndexerBolt.urlFieldParamName, "url");
        config.put(AbstractIndexerBolt.metadataFilterParamName, "key1=value1");

        prepareIndexerBolt(config);

        index(URL, new Metadata());
        Map<String, String> fields = ((DummyIndexer) bolt).returnFields();

        Assert.assertEquals(
                "The document must not pass if the key/value is not found in the metadata",
                0,
                fields.size());
    }

    @Test
    public void testFilterMetadata() throws Exception {
        Map config = new HashMap();
        config.put(AbstractIndexerBolt.urlFieldParamName, "url");

        final List vector = new ArrayList();
        vector.add("parse.title=title");
        vector.add("parse.keywords=keywords");

        config.put(AbstractIndexerBolt.metadata2fieldParamName, vector);

        prepareIndexerBolt(config);

        Metadata metadata = new Metadata();
        metadata.setValue("parse.title", "This is the title");
        metadata.setValue("parse.keywords", "keyword1, keyword2, keyword3");
        metadata.setValue("parse.description", "This is the description");

        index(URL, metadata);
        Map<String, String> fields = ((DummyIndexer) bolt).returnFields();

        Assert.assertArrayEquals(
                "Only the mapped metadata attributes should be indexed",
                new String[] {"keywords", "title", "url"},
                new TreeSet<>(fields.keySet()).toArray());
    }

    @Test
    public void testEmptyFilterMetadata() throws Exception {
        Map config = new HashMap();
        config.put(AbstractIndexerBolt.urlFieldParamName, "url");

        prepareIndexerBolt(config);

        Metadata metadata = new Metadata();
        metadata.setValue("parse.title", "This is the title");
        metadata.setValue("parse.keywords", "keyword1, keyword2, keyword3");
        metadata.setValue("parse.description", "This is the description");

        index(URL, metadata);
        Map<String, String> fields = ((DummyIndexer) bolt).returnFields();

        Assert.assertArrayEquals(
                "Index only the URL if no mapping is provided",
                new String[] {"url"},
                fields.keySet().toArray());
    }
}
