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

package com.digitalpebble.stormcrawler.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.utils.Utils;
import org.yaml.snakeyaml.Yaml;

import clojure.lang.PersistentVector;

/* TODO replace by calls to org.apache.storm.utils.Utils */

public class ConfUtils {

    private ConfUtils() {
    }

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

    /**
     * Return one or more Strings regardless of whether they are represented as
     * a single String or a list in the config.
     **/
    public static List<String> loadListFromConf(String paramKey, Map stormConf) {
        Object obj = stormConf.get(paramKey);
        List<String> list = new LinkedList<>();

        if (obj == null)
            return list;

        if (obj instanceof PersistentVector) {
            list.addAll((PersistentVector) obj);
        } else { // single value?
            list.add(obj.toString());
        }
        return list;
    }

    public static Config loadConf(String resource, Config conf)
            throws FileNotFoundException {
        Yaml yaml = new Yaml();
        Map ret = (Map) yaml.load(new InputStreamReader(new FileInputStream(
                resource), Charset.defaultCharset()));
        if (ret == null) {
            ret = new HashMap();
        }
        // contains a single config element ?
        else {
            ret = extractConfigElement(ret);
        }
        conf.putAll(ret);
        return conf;
    }

    /**
     * If the config consists of a single key 'config', its values are used
     * instead
     **/
    public static Map extractConfigElement(Map conf) {
        if (conf.size() == 1) {
            Object confNode = conf.get("config");
            if (confNode != null && confNode instanceof Map) {
                conf = (Map) confNode;
            }
        }
        return conf;
    }
}
