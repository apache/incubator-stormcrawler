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
package org.apache.stormcrawler.util;

import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.stormcrawler.Metadata;
import org.junit.Assert;
import org.junit.Test;

public class MetadataTransferTest {
    @Test
    public void testTransfer() throws MalformedURLException {
        Map<String, Object> conf = new HashMap<>();
        conf.put(MetadataTransfer.trackDepthParamName, true);
        conf.put(MetadataTransfer.metadataTransferParamName, List.of("cookie.*"));
        MetadataTransfer mdt = MetadataTransfer.getInstance(conf);

        Metadata parentMD = new Metadata();
        parentMD.addValue("cookie.id", "42");
        parentMD.addValue("cookie.source", "example.com");
        parentMD.addValue("fetchInterval", "200");

        Metadata outlinkMD =
                mdt.getMetaForOutlink(
                        "http://www.example.com/outlink.html", "http://www.example.com", parentMD);

        // test the value of track seed, depth and fetch fields
        Assert.assertEquals("1", outlinkMD.getFirstValue(MetadataTransfer.depthKeyName));
        Set<String> expectedFields =
                Set.of(
                        MetadataTransfer.urlPathKeyName,
                        MetadataTransfer.depthKeyName,
                        "cookie.id",
                        "cookie.source");
        Assert.assertEquals(expectedFields, outlinkMD.keySet());
        String[] urlpath = outlinkMD.getValues(MetadataTransfer.urlPathKeyName);
        Assert.assertEquals(1, urlpath.length);
    }

    @Test
    public void testCustomTransferClass() throws MalformedURLException {
        Map<String, Object> conf = new HashMap<>();
        conf.put(MetadataTransfer.metadataTransferClassParamName, "thisclassnameWillNEVERexist");
        boolean hasThrownException = false;
        try {
            MetadataTransfer.getInstance(conf);
        } catch (Exception e) {
            hasThrownException = true;
        }
        Assert.assertEquals(true, hasThrownException);

        conf = new HashMap<>();
        conf.put(
                MetadataTransfer.metadataTransferClassParamName,
                myCustomTransferClass.class.getName());
        hasThrownException = false;
        try {
            MetadataTransfer.getInstance(conf);
        } catch (Exception e) {
            hasThrownException = true;
        }
        Assert.assertEquals(false, hasThrownException);
    }

    @Test
    public void testFilterWithAsterisk() {
        Metadata metadata = new Metadata();
        metadata.addValue("fetch.statusCode", "500");
        metadata.addValue("fetch.error.count", "2");
        metadata.addValue("fetch.exception", "java.lang.Exception");
        metadata.addValue("fetchInterval", "200");
        metadata.addValue("isFeed", "true");
        metadata.addValue("depth", "1");

        // test for empty metadata.persist list
        Map<String, Object> conf = new HashMap<>();
        conf.put(MetadataTransfer.metadataPersistParamName, List.of());
        MetadataTransfer mdt = MetadataTransfer.getInstance(conf);
        Metadata filteredMetadata = mdt.filter(metadata);
        Assert.assertEquals(2, filteredMetadata.size());

        // test for metadata.persist list with asterisk entry
        conf = new HashMap<>();
        conf.put(MetadataTransfer.metadataPersistParamName, List.of("fetch*"));
        mdt = MetadataTransfer.getInstance(conf);
        filteredMetadata = mdt.filter(metadata);
        Assert.assertEquals(5, filteredMetadata.size());

        // test for metadata.persist list with asterisk entry after a dot
        conf = new HashMap<>();
        conf.put(MetadataTransfer.metadataPersistParamName, List.of("fetch.*"));
        mdt = MetadataTransfer.getInstance(conf);
        filteredMetadata = mdt.filter(metadata);
        Assert.assertEquals(4, filteredMetadata.size());

        // test for persist all metadata
        conf = new HashMap<>();
        conf.put(MetadataTransfer.metadataPersistParamName, List.of("*"));
        mdt = MetadataTransfer.getInstance(conf);
        filteredMetadata = mdt.filter(metadata);
        Assert.assertEquals(6, filteredMetadata.size());
    }
}

class myCustomTransferClass extends MetadataTransfer {}
