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
package com.digitalpebble.stormcrawler.persistence;

import java.net.MalformedURLException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.time.DateUtils;
import org.junit.Assert;
import org.junit.Test;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.persistence.DefaultScheduler;
import com.digitalpebble.stormcrawler.persistence.Status;

public class DefaultSchedulerTest {
    @Test
    public void testScheduler() throws MalformedURLException {
        Map<String, Object> stormConf = new HashMap<>();
        stormConf.put("fetchInterval.FETCHED.testKey=someValue", 360);
        stormConf.put("fetchInterval.testKey=someValue", 3600);
        DefaultScheduler scheduler = new DefaultScheduler();
        scheduler.init(stormConf);

        Metadata metadata = new Metadata();
        metadata.addValue("testKey", "someValue");
        Date nextFetch = scheduler.schedule(Status.FETCHED, metadata);

        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.MINUTE, 360);
        Assert.assertEquals(DateUtils.round(cal.getTime(), Calendar.SECOND),
                DateUtils.round(nextFetch, Calendar.SECOND));

        nextFetch = scheduler.schedule(Status.ERROR, metadata);

        cal = Calendar.getInstance();
        cal.add(Calendar.MINUTE, 3600);
        Assert.assertEquals(DateUtils.round(cal.getTime(), Calendar.SECOND),
                DateUtils.round(nextFetch, Calendar.SECOND));
    }

    @Test
    public void testBadConfig() throws MalformedURLException {
        Map<String, Object> stormConf = new HashMap<>();
        stormConf.put("fetchInterval.DODGYSTATUS.testKey=someValue", 360);
        DefaultScheduler scheduler = new DefaultScheduler();
        boolean exception = false;
        try {
            scheduler.init(stormConf);
        } catch (IllegalArgumentException e) {
            exception = true;
        }
        Assert.assertTrue(exception);
    }

    @Test
    public void testNever() throws MalformedURLException {
        Map<String, Object> stormConf = new HashMap<>();
        stormConf.put("fetchInterval.error", -1);
        DefaultScheduler scheduler = new DefaultScheduler();
        scheduler.init(stormConf);

        Metadata metadata = new Metadata();
        Date nextFetch = scheduler.schedule(Status.ERROR, metadata);

        Assert.assertEquals(DefaultScheduler.NEVER, nextFetch);
    }

}
