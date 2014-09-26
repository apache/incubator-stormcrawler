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

package com.digitalpebble.storm.crawler.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/** Utility class to simplify the manipulation of Maps of Strings **/
public class KeyValues {

    public static String getValue(String key, Map<String, String[]> md) {
        String[] values = md.get(key);
        if (values == null)
            return null;
        if (values.length == 0)
            return null;
        return values[0];
    }

    public static void addValues(String key, Map<String, String[]> md,
            Collection<String> values) {
        if (values == null || values.size() == 0)
            return;
        String[] vals = md.get(key);
        if (vals == null) {
            md.put(key, values.toArray(new String[values.size()]));
            return;
        }
        List<String> existing = Arrays.asList(vals);
        existing.addAll(values);
        md.put(key, existing.toArray(new String[existing.size()]));
    }

}
