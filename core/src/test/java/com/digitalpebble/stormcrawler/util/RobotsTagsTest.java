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
import org.junit.Assert;
import org.junit.Test;

public class RobotsTagsTest {
    @Test
    public void testHTTPHeaders() throws MalformedURLException {
        Metadata md = new Metadata();
        RobotsTags tags = new RobotsTags(md, "");
        Assert.assertEquals(false, tags.isNoCache());
        Assert.assertEquals(false, tags.isNoFollow());
        Assert.assertEquals(false, tags.isNoIndex());

        md = new Metadata();
        md.setValue("X-Robots-Tag", "none");
        tags = new RobotsTags(md, "");
        Assert.assertEquals(true, tags.isNoCache());
        Assert.assertEquals(true, tags.isNoFollow());
        Assert.assertEquals(true, tags.isNoIndex());

        md = new Metadata();
        md.setValues("X-Robots-Tag", new String[] {"noindex", "nofollow"});
        tags = new RobotsTags(md, "");
        Assert.assertEquals(false, tags.isNoCache());
        Assert.assertEquals(true, tags.isNoFollow());
        Assert.assertEquals(true, tags.isNoIndex());

        // expect the content to be incorrect
        md = new Metadata();
        md.setValue("X-Robots-Tag", "noindex, nofollow");
        tags = new RobotsTags(md, "");
        Assert.assertEquals(false, tags.isNoCache());
        Assert.assertEquals(true, tags.isNoFollow());
        Assert.assertEquals(true, tags.isNoIndex());
    }
}
