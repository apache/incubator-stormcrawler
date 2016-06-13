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
package com.digitalpebble.storm.crawler.persistence;

import java.net.MalformedURLException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.time.DateUtils;
import org.junit.Assert;
import org.junit.Test;

import com.digitalpebble.storm.crawler.Metadata;

public class DefaultSchedulerTest {
    @Test
    public void testScheduler() throws MalformedURLException {
        Map<String, Object> stormConf = new HashMap<>();
        stormConf.put("fetchInterval.testKey=someValue", 3600);
        DefaultScheduler scheduler = new DefaultScheduler();
        scheduler.init(stormConf);

        Metadata metadata = new Metadata();
        metadata.addValue("testKey", "someValue");
        Date nextFetch = scheduler.schedule(Status.FETCHED, metadata);

        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.MINUTE, 3600);
        Assert.assertEquals(DateUtils.round(cal.getTime(), Calendar.SECOND),
                DateUtils.round(nextFetch, Calendar.SECOND));
    }

}
