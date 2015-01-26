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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

import backtype.storm.Config;
import backtype.storm.utils.Utils;

/* TODO replace by calls to backtype.storm.utils.Utils */

public class ConfUtils {

    public static int getInt(Map<String, Object> conf, String key,
            int defaultValue) {
        Object obj = Utils.get(conf, key, defaultValue);
        return Utils.getInt(obj);
    }

    public static long getLong(Map<String, Object> conf, String key,
            long defaultValue) {
        return (Long) Utils.get(conf, key, defaultValue);
    }

    public static float getFloat(Map<String, Object> conf, String key,
            float defaultValue) {
        Object obj = Utils.get(conf, key, defaultValue);
        if (obj instanceof Double)
            return ((Double) obj).floatValue();
        return (Float) obj;
    }

    public static boolean getBoolean(Map<String, Object> conf, String key,
            boolean defaultValue) {
        Object obj = Utils.get(conf, key, defaultValue);
        return Utils.getBoolean(obj, defaultValue);
    }

    public static String getString(Map<String, Object> conf, String key) {
        return (String) Utils.get(conf, key, null);
    }

    public static String getString(Map<String, Object> conf, String key,
            String defaultValue) {
        return (String) Utils.get(conf, key, defaultValue);
    }

    public static Config loadConf(String resource) throws FileNotFoundException {
        Config conf = new Config();
        Yaml yaml = new Yaml();
        Map ret = (Map) yaml.load(new InputStreamReader(new FileInputStream(
                resource), Charset.defaultCharset()));
        if (ret == null) {
            ret = new HashMap();
        }
        conf.putAll(ret);
        return conf;
    }
}
