/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.stormcrawler.protocol;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class HttpHeadersTest {

    @Test
    void testHttpDate() {
        String[][] dates = { //
            //
            {"Tue, 22 Sep 2020 08:00:00 GMT", "2020-09-22T08:00:00.000Z"}, //
            {"Sun, 06 Nov 1994 08:49:37 GMT", "1994-11-06T08:49:37.000Z"}, //
            {"Sun, 06 Nov 1994 20:49:37 GMT", "1994-11-06T20:49:37.000Z"}
        };
        for (int i = 0; i < dates.length; i++) {
            Assertions.assertEquals(dates[i][0], HttpHeaders.formatHttpDate(dates[i][1]));
        }
    }
}
