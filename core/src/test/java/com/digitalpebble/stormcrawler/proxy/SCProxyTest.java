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
package com.digitalpebble.stormcrawler.proxy;

import org.junit.Assert;
import org.junit.Test;

public class SCProxyTest {
    @Test
    public void testProxyConstructor() {
        String[] valid_inputs = {
            "http://example.com:8080",
            "https://example.com:8080",
            "http://user1:pass1@example.com:8080",
            "sock5://user1:pass1@example.com:8080",
            "http://example.com:80",
            "sock5://example.com:64000",
        };

        String[][] valid_outputs = {
            {"http", null, null, "example.com", "8080"},
            {"https", null, null, "example.com", "8080"},
            {"http", "user1", "pass1", "example.com", "8080"},
            {"sock5", "user1", "pass1", "example.com", "8080"},
            {"http", null, null, "example.com", "80"},
            {"sock5", null, null, "example.com", "64000"},
        };

        String[] invalid_inputs = {
            "http://example.com",
            "sock5://:example.com:8080",
            "example.com:8080",
            "https://user1@example.com:8080",
        };

        for (int i = 0; i < valid_inputs.length; i++) {
            SCProxy proxy = new SCProxy(valid_inputs[i]);

            Assert.assertEquals(proxy.getUsage(), 0);

            Assert.assertEquals(proxy.getProtocol(), valid_outputs[i][0]);
            Assert.assertEquals(proxy.getUsername(), valid_outputs[i][1]);
            Assert.assertEquals(proxy.getPassword(), valid_outputs[i][2]);
            Assert.assertEquals(proxy.getAddress(), valid_outputs[i][3]);
            Assert.assertEquals(proxy.getPort(), valid_outputs[i][4]);
        }

        for (String invalid_input : invalid_inputs) {
            boolean failed = false;

            try {
                new SCProxy(invalid_input);
            } catch (IllegalArgumentException ignored) {
                failed = true;
            }

            Assert.assertTrue(failed);
        }
    }

    @Test
    public void testToString() {
        String[] proxyStrings = {
            "http://example.com:8080",
            "https://example.com:8080",
            "http://user1:pass1@example.com:8080",
            "sock5://user1:pass1@example.com:8080",
            "http://example.com:80",
            "sock5://example.com:64000",
        };

        for (String proxyString : proxyStrings) {
            SCProxy proxy = new SCProxy(proxyString);
            Assert.assertEquals(proxyString, proxy.toString());
        }
    }

    @Test
    public void testIncrementUsage() {
        SCProxy proxy = new SCProxy("http://user1:pass1@example.com:8080");
        Assert.assertEquals(proxy.getUsage(), 0);
        proxy.incrementUsage();
        Assert.assertEquals(proxy.getUsage(), 1);
    }
}
