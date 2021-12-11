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
package com.digitalpebble.stormcrawler;

import com.esotericsoftware.kryo.serializers.DefaultArraySerializers.StringArraySerializer;
import com.esotericsoftware.kryo.serializers.DefaultSerializers.StringSerializer;
import com.esotericsoftware.kryo.serializers.MapSerializer.BindMap;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** Wrapper around Map &lt;String,String[]&gt; * */
public class Metadata {

    // customize the behaviour of Kryo via annotations
    @SuppressWarnings("FieldMayBeFinal")
    @BindMap(
            valueSerializer = StringArraySerializer.class,
            keySerializer = StringSerializer.class,
            valueClass = String[].class,
            keyClass = String.class,
            keysCanBeNull = false)
    private Map<String, String[]> md;

    private transient boolean locked = false;

    public static final Metadata empty = new Metadata(Collections.<String, String[]>emptyMap());

    public Metadata() {
        md = new HashMap<>();
    }

    /** Wraps an existing HashMap into a Metadata object - does not clone the content */
    public Metadata(@NotNull Map<String, String[]> metadata) {
        md = Objects.requireNonNull(metadata);
    }

    /** Puts all the metadata into the current instance * */
    public void putAll(@NotNull Metadata m) {
        checkLockException();
        md.putAll(m.md);
    }

    /**
     * Puts all prefixed metadata into the current instance
     *
     * @param m metadata to be added
     * @param prefix string to prefix keys in m before adding them to the current metadata. No
     *     separator is inserted between prefix and original key, so the prefix must include any
     *     separator (eg. a dot)
     */
    public void putAll(@NotNull Metadata m, @Nullable String prefix) {
        if (prefix == null || prefix.isEmpty()) {
            putAll(m);
            return;
        }
        m.md.forEach((k, v) -> setValues(prefix + k, v));
    }

    /** @return the first value for the key or null if it does not exist * */
    public String getFirstValue(@NotNull String key) {
        String[] values = md.get(key);
        if (values == null) return null;
        if (values.length == 0) return null;
        return values[0];
    }

    @Nullable
    /** @return the first value for the key or null if it does not exist, given a prefix */
    public String getFirstValue(@NotNull String key, @Nullable String prefix) {
        if (prefix == null || prefix.length() == 0) return getFirstValue(key);
        return getFirstValue(prefix + key);
    }

    @Nullable
    public String[] getValues(@NotNull String key, @Nullable String prefix) {
        if (prefix == null || prefix.length() == 0) return getValues(key);
        return getValues(prefix + key);
    }

    @Nullable
    public String[] getValues(@NotNull String key) {
        String[] values = md.get(key);
        if (values == null) return null;
        if (values.length == 0) return null;
        return values;
    }

    public boolean containsKey(@NotNull String key) {
        return md.containsKey(key);
    }

    public boolean containsKeyWithValue(@NotNull String key, String value) {
        String[] values = getValues(key);
        if (values == null) return false;
        for (String s : values) {
            if (s.equals(value)) return true;
        }
        return false;
    }

    /** Set the value for a given key. The value can be null. */
    public void setValue(@NotNull String key, @Nullable String value) {
        checkLockException();
        md.put(key, new String[] {value});
    }

    /** Set the value for a given key. The value can be null. */
    public void setValues(@NotNull String key, @Nullable String[] values) {
        checkLockException();
        if (values == null || values.length == 0) return;
        md.put(key, values);
    }

    /** Add or set the value for a given key. The value can be null. */
    public void addValue(@NotNull String key, @Nullable String value) {
        checkLockException();

        if (StringUtils.isBlank(value)) return;

        String[] existingvals = md.get(key);
        if (existingvals == null || existingvals.length == 0) {
            setValue(key, value);
            return;
        }

        int currentLength = existingvals.length;
        String[] newvals = new String[currentLength + 1];
        newvals[currentLength] = value;
        System.arraycopy(existingvals, 0, newvals, 0, currentLength);
        md.put(key, newvals);
    }

    /** Add or set the value for a given key. The values can be null. */
    public void addValues(@NotNull String key, @Nullable String... values) {
        checkLockException();

        if (values == null || values.length == 0) return;
        String[] existingvals = md.get(key);
        if (existingvals == null) {
            md.put(key, values.clone());
            return;
        }

        String[] newValue = Arrays.copyOf(values, existingvals.length + values.length);
        System.arraycopy(values, 0, newValue, existingvals.length, values.length);
        md.put(key, newValue);
    }

    /** Add or set the value for a given key. The value can be null. */
    public void addValues(@NotNull String key, @Nullable Collection<String> values) {
        String[] tmp = null;
        if (values != null) {
            tmp = values.toArray(new String[0]);
        }
        addValues(key, tmp);
    }

    /** @return the previous value(s) associated with <tt>key</tt> */
    public String[] remove(@NotNull String key) {
        checkLockException();
        return md.remove(key);
    }

    public String toString() {
        return toString("");
    }

    /** Returns a String representation of the metadata with one K/V per line */
    public String toString(String prefix) {
        StringBuilder sb = new StringBuilder();
        if (prefix == null) prefix = "";
        for (Entry<String, String[]> entry : md.entrySet()) {
            for (String val : entry.getValue()) {
                sb.append(prefix).append(entry.getKey()).append(": ").append(val).append("\n");
            }
        }
        return sb.toString();
    }

    public int size() {
        return md.size();
    }

    public Set<String> keySet() {
        return md.keySet();
    }

    /** Returns the first non empty value found for the keys or null if none found. */
    public static String getFirstValue(Metadata md, String... keys) {
        for (String key : keys) {
            String val = md.getFirstValue(key);
            if (StringUtils.isBlank(val)) continue;
            return val;
        }
        return null;
    }

    /**
     * Returns the underlying Map
     *
     * @deprecated replace with getMap, getShallowCopyOfMap or getDeepCopyOfMap.
     */
    @Deprecated
    public Map<String, String[]> asMap() {
        return md;
    }

    /** Returns the underlying map. */
    public Map<String, String[]> getMap() {
        return md;
    }

    /**
     * Get a shallow copy of the underlying map. Changes to the keys are not changing the original
     * metadata, but changing the values does change the original metadata.
     */
    public Map<String, String[]> getShallowCopyOfMap() {
        return new HashMap<>(md);
    }

    /**
     * Get a deep copy of the underlying map. Changes to the keys and values are not changing the
     * original metadata.
     */
    public Map<String, String[]> getDeepCopyOfMap() {
        return md.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Entry::getKey,
                                e -> {
                                    String[] origin = e.getValue();
                                    String[] copy = new String[origin.length];
                                    System.arraycopy(origin, 0, copy, 0, origin.length);
                                    return copy;
                                }));
    }

    /**
     * Creates a plain copy of this with the same underlying map. Therefor all changes done to the
     * new instance are also done to the origin metadata
     *
     * @return a copy of this
     */
    public Metadata copy() {
        return new Metadata(md);
    }

    /**
     * Create a shallow or deep copy of this, depending on the {@code shallow} parameter. When
     * creating a shallow copy: Changes to the keys are not changing the original metadata, but
     * changing the values does change the original metadata. When creating a deep copy: Changes to
     * the keys and values are not changing the original metadata.
     *
     * @param shallow if true creates a shallow copy, otherwise it creates a deep copy
     * @return a shallow or deep copy of this
     */
    public Metadata copy(boolean shallow) {
        if (shallow) return copyShallow();
        else return copyDeep();
    }

    /**
     * Get a shallow copy of this. Changes to the keys are not changing the original metadata, but
     * changing the values does change the original metadata.
     */
    public Metadata copyShallow() {
        return new Metadata(getShallowCopyOfMap());
    }

    /**
     * Get a deep copy of this. Changes to the keys and values are not changing the original
     * metadata.
     */
    public Metadata copyDeep() {
        return new Metadata(getDeepCopyOfMap());
    }

    /**
     * Prevents modifications to the metadata object. Useful for debugging modifications of the
     * metadata after they have been serialized. Instead of choking when serializing, a
     * ConcurrentModificationException will be thrown where the metadata are modified. <br>
     * Use like this in any bolt where you see java.lang.RuntimeException:
     * com.esotericsoftware.kryo.KryoException: java.util.ConcurrentModificationException<br>
     * collector.emit(StatusStreamName, tuple, new Values(url, metadata.lock(), Status.FETCHED));
     *
     * @since 1.16
     */
    public Metadata lock() {
        locked = true;
        return this;
    }

    /**
     * Release the lock on a metadata
     *
     * @since 1.16
     */
    public Metadata unlock() {
        locked = false;
        return this;
    }

    /** @since 1.16 */
    private void checkLockException() {
        if (locked)
            throw new ConcurrentModificationException(
                    "Attempt to modify a metadata after it has been sent to the serializer");
    }
}
