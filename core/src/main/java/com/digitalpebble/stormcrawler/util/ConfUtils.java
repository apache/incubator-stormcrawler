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

import static org.apache.storm.utils.Utils.findAndReadConfigFile;

import java.io.FileNotFoundException;
import java.util.*;
import org.apache.storm.Config;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class ConfUtils {

    private ConfUtils() {}

    public static int getInt(
            @NotNull Map<String, Object> conf, @NotNull String key, int defaultValue) {
        Object ret = conf.get(key);
        if (ret == null) {
            ret = defaultValue;
        }
        return ((Number) ret).intValue();
    }

    public static long getLong(
            @NotNull Map<String, Object> conf, @NotNull String key, long defaultValue) {
        Object ret = conf.get(key);
        if (ret == null) {
            ret = defaultValue;
        }
        return ((Number) ret).longValue();
    }

    public static float getFloat(
            @NotNull Map<String, Object> conf, @NotNull String key, float defaultValue) {
        Object ret = conf.get(key);
        if (ret == null) {
            ret = defaultValue;
        }
        return ((Number) ret).floatValue();
    }

    public static boolean getBoolean(
            @NotNull Map<String, Object> conf, @NotNull String key, boolean defaultValue) {
        Object ret = conf.get(key);
        if (ret == null) {
            ret = defaultValue;
        }
        return (Boolean) ret;
    }

    @Nullable
    public static String getString(@NotNull Map<String, Object> conf, @NotNull String key) {
        return (String) conf.get(key);
    }

    public static String getString(
            @NotNull Map<String, Object> conf, @NotNull String key, @Nullable String defaultValue) {
        Object ret = conf.get(key);
        if (ret == null) {
            ret = defaultValue;
        }
        return (String) ret;
    }

    @Nullable
    @Contract(value = "_, null -> null")
    private static <T extends Enum<T>> T convertToEnumOrNull(
            @NotNull Class<T> enumClass, @Nullable Object toConvert) {

        if (toConvert == null) return null;

        if (toConvert instanceof String) {
            return Enum.valueOf(enumClass, (String) toConvert);
        } else if (enumClass.isInstance(toConvert)) {
            //noinspection unchecked
            return (T) toConvert;
        } else {
            return null;
        }
    }

    @NotNull
    private static <T extends Enum<T>> T convertToEnum(
            @NotNull Class<T> enumClass, @NotNull Object toConvert) {
        T retVal = convertToEnumOrNull(enumClass, toConvert);
        if (retVal == null) {
            throw new RuntimeException(
                    String.format("Can not convert %s to %s", toConvert, enumClass.getName()));
        }
        return retVal;
    }

    @NotNull
    public static <T extends Enum<T>> T getEnum(
            @NotNull Map<String, Object> conf, @NotNull String key, @NotNull Class<T> enumClass) {
        Object value = Objects.requireNonNull(conf.get(key));
        return convertToEnum(enumClass, value);
    }

    public static <T extends Enum<T>> T getEnumOrDefault(
            @NotNull Map<String, Object> conf,
            @NotNull String key,
            @Nullable T defaultValue,
            @NotNull Class<T> enumClass) {
        Object value = conf.get(key);
        if (value == null) {
            return defaultValue;
        } else {
            return convertToEnumOrNull(enumClass, value);
        }
    }

    /**
     * Returns either a collection of objects or throws a RuntimeException. If there is only one
     * value it <b>throws a RuntimeException</b>.
     *
     * @see ConfUtils {@link ConfUtils#loadCollection(Map, String)} for a single friendly
     *     alternative.
     */
    @NotNull
    public static Collection<Object> getCollection(
            @NotNull Map<String, Object> conf, @NotNull String key) {
        Collection<Object> obj = getCollectionOrNull(conf, key);
        if (obj == null) {
            throw new RuntimeException(
                    String.format("There is no Collection value for the key '%s'.", key));
        }
        return obj;
    }

    /**
     * Returns either a collection of objects or null. If there is only one value it also returns
     * null.
     *
     * @see ConfUtils {@link ConfUtils#loadCollectionOrNull(Map, String)} for a single friendly
     *     alternative.
     */
    @Nullable
    public static Collection<Object> getCollectionOrNull(
            @NotNull Map<String, Object> conf, @NotNull String key) {
        Object obj = conf.get(key);
        if (obj == null) {
            return null;
        }
        //noinspection unchecked
        return (Collection<Object>) obj;
    }

    /**
     * Returns either a collection of objects or throws a RuntimeException. If there is only one
     * value, it will return a SingletonList.
     */
    @NotNull
    public static Collection<Object> loadCollection(
            @NotNull Map<String, Object> conf, @NotNull String key) {
        Collection<Object> collection = loadCollectionOrNull(conf, key);
        if (collection == null) {
            throw new RuntimeException(
                    String.format("There is no Collection value for the key '%s'.", key));
        }
        return collection;
    }

    /**
     * Returns either a collection ob objects or null. If there is only one value, it will return a
     * SingletonList.
     */
    @Nullable
    public static Collection<Object> loadCollectionOrNull(
            @NotNull Map<String, Object> conf, @NotNull String key) {
        Object obj = conf.get(key);
        if (obj == null) {
            return null;
        }
        if (obj instanceof Collection) {
            //noinspection unchecked
            return (Collection<Object>) obj;
        } else {
            return Collections.singletonList(obj);
        }
    }

    @Nullable
    public static Map<String, Object> getSubConfig(
            @NotNull Map<String, Object> conf, @NotNull String key) {
        Object obj = conf.get(key);
        if (obj == null) {
            return null;
        }
        //noinspection unchecked
        return (Map<String, Object>) obj;
    }

    /**
     * Returns either a Map or a reference to {@link Collections#EMPTY_MAP}. If {@code key} is not
     * in {@code conf} it returns null.
     */
    @Nullable
    public static Map<String, Object> loadSubConfigOrNull(
            @NotNull Map<String, Object> conf, @NotNull String key) {
        Object obj = conf.get(key);
        if (obj == null) {
            return null;
        }
        if (obj instanceof Map) {
            //noinspection unchecked
            return (Map<String, Object>) obj;
        } else {
            return Collections.emptyMap();
        }
    }

    /**
     * Returns either a Map or a reference to {@link Collections#EMPTY_MAP}. If {@code key} is not
     * in {@code conf} it trows a RuntimeCollection.
     */
    @NotNull
    public static Map<String, Object> loadSubConfig(
            @NotNull Map<String, Object> conf, @NotNull String key) {
        Map<String, Object> subConfig = loadSubConfigOrNull(conf, key);
        if (subConfig == null) {
            throw new RuntimeException(String.format("There is no SubConfig at key '%s'.", key));
        }
        return subConfig;
    }

    /**
     * Return one or more Strings regardless of whether they are represented as a single String or a
     * list in the config or an empty List if no value could be found for that key.
     */
    @NotNull
    public static List<String> loadListFromConf(
            @NotNull Map<String, Object> stormConf, @NotNull String key) {
        Object obj = stormConf.get(key);
        List<String> list = new ArrayList<>();

        if (obj == null) return list;

        if (obj instanceof Collection) {
            list.addAll((Collection<String>) obj);
        } else { // single value?
            list.add(obj.toString());
        }
        return list;
    }

    @Deprecated
    @Contract()
    public static Config loadConf(String resource, Config conf) throws FileNotFoundException {
        loadConfigIntoTarget(resource, conf);
        return conf;
    }

    /** Loads the resource at {@code pathToResource} into the given {@code target}. */
    public static void loadConfigIntoTarget(
            @NotNull String pathToResource, @NotNull Config target) {
        Map<String, Object> ret = findAndReadConfigFile(pathToResource);

        if (ret.isEmpty()) {
            return;
        }

        // contains a single config element ?
        ret = extractConfigElement(ret);
        target.putAll(ret);
    }

    /** If the config consists of a single key 'config', its values are used instead */
    public static Map<String, Object> extractConfigElement(Map<String, Object> conf) {
        if (conf.size() == 1) {
            Object confNode = conf.get("config");
            if (confNode instanceof Map) {
                conf = (Map<String, Object>) confNode;
            }
        }
        return conf;
    }
}
