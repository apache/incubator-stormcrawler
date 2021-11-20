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

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.storm.Config;
import org.junit.Assert;
import org.junit.Test;

public class MultiProxyManagerTest {
    @Test
    public void testMultiProxyManagerConstructorArray() {
        String[] proxyStrings = {
            "http://example.com:8080",
            "https://example.com:8080",
            "http://user1:pass1@example.com:8080",
            "sock5://user1:pass1@example.com:8080",
            "http://example.com:80",
            "sock5://example.com:64000",
        };

        MultiProxyManager pm = new MultiProxyManager();
        pm.configure(MultiProxyManager.ProxyRotation.RANDOM, proxyStrings);

        Assert.assertEquals(pm.proxyCount(), proxyStrings.length);
    }

    @Test
    public void testMultiProxyManagerConstructorFile() throws IOException {
        String[] proxyStrings = {
            "http://example.com:8080",
            "https://example.com:8080",
            "http://user1:pass1@example.com:8080",
            "sock5://user1:pass1@example.com:8080",
            "http://example.com:80",
            "sock5://example.com:64000",
        };

        FileWriter writer = new FileWriter("/tmp/proxies.txt");
        for (String proxyString : proxyStrings) {
            writer.write("# fake comment to test" + "\n");
            writer.write("// fake comment to test" + "\n");
            writer.write("       " + "\n");
            writer.write("\n");
            writer.write(proxyString + "\n");
        }
        writer.close();

        Config config = new Config();
        config.put("http.proxy.file", "/tmp/proxies.txt");
        config.put("http.proxy.rotation", "ROUND_ROBIN");

        MultiProxyManager pm = new MultiProxyManager();
        pm.configure(config);

        Assert.assertEquals(pm.proxyCount(), proxyStrings.length);

        Files.deleteIfExists(Paths.get("/tmp/proxies.txt"));
    }

    @Test
    public void testGetRandom() {
        String[] proxyStrings = {
            "http://example.com:8080",
            "https://example.com:8080",
            "http://user1:pass1@example.com:8080",
            "sock5://user1:pass1@example.com:8080",
            "http://example.com:80",
            "sock5://example.com:64000",
        };

        MultiProxyManager pm = new MultiProxyManager();
        pm.configure(MultiProxyManager.ProxyRotation.RANDOM, proxyStrings);

        for (int i = 0; i < 1000; i++) {
            SCProxy proxy = pm.getProxy(null);
            Assert.assertTrue(proxy.toString().length() > 0);
        }
    }

    @Test
    public void testGetRoundRobin() {
        String[] proxyStrings = {
            "http://example.com:8080",
            "https://example.com:8080",
            "http://user1:pass1@example.com:8080",
            "sock5://user1:pass1@example.com:8080",
            "http://example.com:80",
            "sock5://example.com:64000",
        };

        MultiProxyManager pm = new MultiProxyManager();
        pm.configure(MultiProxyManager.ProxyRotation.ROUND_ROBIN, proxyStrings);

        SCProxy proxy1 = pm.getProxy(null);
        SCProxy proxy2 = pm.getProxy(null);
        SCProxy proxy3 = pm.getProxy(null);

        Assert.assertNotEquals(proxy1.toString(), proxy2.toString());
        Assert.assertNotEquals(proxy1.toString(), proxy3.toString());
        Assert.assertNotEquals(proxy2.toString(), proxy1.toString());
        Assert.assertNotEquals(proxy2.toString(), proxy3.toString());
        Assert.assertNotEquals(proxy3.toString(), proxy1.toString());
        Assert.assertNotEquals(proxy3.toString(), proxy2.toString());

        for (int i = 0; i < 3; i++) {
            pm.getProxy(null);
        }

        SCProxy proxy4 = pm.getProxy(null);
        SCProxy proxy5 = pm.getProxy(null);
        SCProxy proxy6 = pm.getProxy(null);

        Assert.assertNotEquals(proxy4.toString(), proxy5.toString());
        Assert.assertNotEquals(proxy4.toString(), proxy6.toString());
        Assert.assertNotEquals(proxy5.toString(), proxy4.toString());
        Assert.assertNotEquals(proxy5.toString(), proxy6.toString());
        Assert.assertNotEquals(proxy6.toString(), proxy4.toString());
        Assert.assertNotEquals(proxy6.toString(), proxy5.toString());

        Assert.assertEquals(proxy1.toString(), proxy4.toString());
        Assert.assertEquals(proxy2.toString(), proxy5.toString());
        Assert.assertEquals(proxy3.toString(), proxy6.toString());
    }

    @Test
    public void testGetLeastUsed() {
        String[] proxyStrings = {
            "http://example.com:8080",
            "https://example.com:8080",
            "http://user1:pass1@example.com:8080",
            "sock5://user1:pass1@example.com:8080",
            "http://example.com:80",
            "sock5://example.com:64000",
        };

        MultiProxyManager pm = new MultiProxyManager();
        pm.configure(MultiProxyManager.ProxyRotation.LEAST_USED, proxyStrings);

        SCProxy proxy1 = pm.getProxy(null);
        SCProxy proxy2 = pm.getProxy(null);
        SCProxy proxy3 = pm.getProxy(null);

        Assert.assertNotEquals(proxy1.toString(), proxy2.toString());
        Assert.assertNotEquals(proxy1.toString(), proxy3.toString());
        Assert.assertNotEquals(proxy2.toString(), proxy1.toString());
        Assert.assertNotEquals(proxy2.toString(), proxy3.toString());
        Assert.assertNotEquals(proxy3.toString(), proxy1.toString());
        Assert.assertNotEquals(proxy3.toString(), proxy2.toString());

        Assert.assertEquals(proxy1.getUsage(), 1);
        Assert.assertEquals(proxy2.getUsage(), 1);
        Assert.assertEquals(proxy3.getUsage(), 1);

        for (int i = 0; i < 3; i++) {
            pm.getProxy(null);
        }

        SCProxy proxy4 = pm.getProxy(null);
        SCProxy proxy5 = pm.getProxy(null);
        SCProxy proxy6 = pm.getProxy(null);

        Assert.assertNotEquals(proxy4.toString(), proxy5.toString());
        Assert.assertNotEquals(proxy4.toString(), proxy6.toString());
        Assert.assertNotEquals(proxy5.toString(), proxy4.toString());
        Assert.assertNotEquals(proxy5.toString(), proxy6.toString());
        Assert.assertNotEquals(proxy6.toString(), proxy4.toString());
        Assert.assertNotEquals(proxy6.toString(), proxy5.toString());

        Assert.assertEquals(proxy4.getUsage(), 2);
        Assert.assertEquals(proxy5.getUsage(), 2);
        Assert.assertEquals(proxy6.getUsage(), 2);

        Assert.assertEquals(proxy1.toString(), proxy4.toString());
        Assert.assertEquals(proxy2.toString(), proxy5.toString());
        Assert.assertEquals(proxy3.toString(), proxy6.toString());
    }
}
