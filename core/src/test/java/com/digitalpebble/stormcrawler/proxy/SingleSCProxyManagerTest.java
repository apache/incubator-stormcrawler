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

package com.digitalpebble.stormcrawler.proxy;

import org.junit.Test;
import org.junit.Assert;

import java.io.FileNotFoundException;

public class SingleSCProxyManagerTest {
    @Test
    public void testSimpleProxyManager() throws FileNotFoundException, IllegalArgumentException {
        SingleProxyManager pm = new SingleProxyManager();
        pm.configure(ProxyRotation.ROUND_ROBIN, "http://user1:pass1@example.com:8080");

        Assert.assertTrue(pm.ready());

        SCProxy proxy = pm.getProxy();

        Assert.assertEquals(proxy.protocol, "http");
        Assert.assertEquals(proxy.address, "example.com");
        Assert.assertEquals(proxy.port, "8080");
        Assert.assertEquals(proxy.username, "user1");
        Assert.assertEquals(proxy.password, "pass1");

        Assert.assertEquals(proxy.toString(), "http://user1:pass1@example.com:8080");
    }
}
