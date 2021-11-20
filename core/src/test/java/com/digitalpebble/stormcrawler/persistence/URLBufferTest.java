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
package com.digitalpebble.stormcrawler.persistence;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.persistence.urlbuffer.PriorityURLBuffer;
import com.digitalpebble.stormcrawler.persistence.urlbuffer.SimpleURLBuffer;
import com.digitalpebble.stormcrawler.persistence.urlbuffer.URLBuffer;
import java.net.MalformedURLException;
import org.junit.Assert;
import org.junit.Test;

public class URLBufferTest {
    @Test
    public void testSimpleURLBuffer() throws MalformedURLException {
        URLBuffer buffer = new SimpleURLBuffer();
        Assert.assertFalse(buffer.hasNext());
        buffer.add("http://a.net/test.html", new Metadata());
        buffer.add("http://a.net/test2.html", new Metadata());
        buffer.add("http://b.net/test.html", new Metadata());
        buffer.add("http://c.net/test.html", new Metadata());
        Assert.assertEquals("http://a.net/test.html", buffer.next().get(0));
        Assert.assertEquals("http://b.net/test.html", buffer.next().get(0));
        // should return false if already there
        boolean added = buffer.add("http://c.net/test.html", new Metadata());
        Assert.assertFalse(added);
        added = buffer.add("http://d.net/test.html", new Metadata());
        Assert.assertTrue(added);
        Assert.assertEquals("http://c.net/test.html", buffer.next().get(0));
        Assert.assertEquals("http://a.net/test2.html", buffer.next().get(0));
        Assert.assertEquals("http://d.net/test.html", buffer.next().get(0));
        Assert.assertFalse(buffer.hasNext());
    }

    @Test
    public void testPriorityBuffer() throws MalformedURLException, InterruptedException {
        URLBuffer buffer = new PriorityURLBuffer();
        Assert.assertFalse(buffer.hasNext());
        buffer.add("http://a.net/test.html", new Metadata());
        buffer.add("http://a.net/test2.html", new Metadata());
        buffer.add("http://b.net/test.html", new Metadata());
        buffer.add("http://c.net/test.html", new Metadata());

        buffer.acked("http://c.net/test.html");
        buffer.acked("http://c.net/test.html");
        buffer.acked("http://c.net/test.html");

        buffer.acked("http://b.net/test.html");

        // wait N seconds - should trigger a rerank
        Thread.sleep(10000);

        // c should come first - it has been acked more often
        Assert.assertEquals("http://c.net/test.html", buffer.next().get(0));

        // then b
        Assert.assertEquals("http://b.net/test.html", buffer.next().get(0));

        // then a
        Assert.assertEquals("http://a.net/test.html", buffer.next().get(0));

        Assert.assertEquals("http://a.net/test2.html", buffer.next().get(0));

        Assert.assertFalse(buffer.hasNext());
    }
}
