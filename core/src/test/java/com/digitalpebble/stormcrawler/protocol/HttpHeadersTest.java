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
package com.digitalpebble.stormcrawler.protocol;

import org.junit.Assert;
import org.junit.Test;

public class HttpHeadersTest {

    @Test
    public void testHttpDate() {
        String[][] dates = { //
            {"Tue, 22 Sep 2020 08:00:00 GMT", "2020-09-22T08:00:00.000Z"}, //
            {"Sun, 06 Nov 1994 08:49:37 GMT", "1994-11-06T08:49:37.000Z"}, //
            {"Sun, 06 Nov 1994 20:49:37 GMT", "1994-11-06T20:49:37.000Z"}, //
        };
        for (int i = 0; i < dates.length; i++) {
            Assert.assertEquals(dates[i][0], HttpHeaders.formatHttpDate(dates[i][1]));
        }
    }
}
