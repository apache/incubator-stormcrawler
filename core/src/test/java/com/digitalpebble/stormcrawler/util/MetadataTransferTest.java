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
package com.digitalpebble.stormcrawler.util;

import com.digitalpebble.stormcrawler.Metadata;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class MetadataTransferTest {
    @Test
    public void testTransfer() throws MalformedURLException {
        Map<String, Object> conf = new HashMap<>();
        conf.put(MetadataTransfer.trackDepthParamName, true);
        MetadataTransfer mdt = MetadataTransfer.getInstance(conf);
        Metadata parentMD = new Metadata();
        Metadata outlinkMD =
                mdt.getMetaForOutlink(
                        "http://www.example.com/outlink.html", "http://www.example.com", parentMD);
        // test the value of track seed and depth
        Assert.assertEquals("1", outlinkMD.getFirstValue(MetadataTransfer.depthKeyName));
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
}

class myCustomTransferClass extends MetadataTransfer {}
