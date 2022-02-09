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

import static java.util.Collections.EMPTY_MAP;

import com.digitalpebble.stormcrawler.util.MetadataKryoSerializer;
import com.esotericsoftware.kryo.DefaultSerializer;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.Unmodifiable;

/** Wrapper around Map &lt;String,String[]&gt; * */
@DefaultSerializer(MetadataKryoSerializer.class)
public class Metadata {

    // The content of the metadata
    private final @NotNull Map<@NotNull String, String @NotNull []> map;

    // Make it transient to protect from serialisation.
    private transient boolean locked = false;

    /** An empty Metadata instance, readonly. */
    @NotNull @Unmodifiable
    public static final Metadata EMPTY_METADATA = new Metadata(Collections.emptyMap());

    /**
     * An empty Metadata instance, readonly.
     *
     * @deprecated use {@link Metadata#EMPTY_METADATA} or {@link Metadata#emptyMetadata()} instead.
     */
    @Deprecated @NotNull @Unmodifiable public static final Metadata empty = EMPTY_METADATA;

    /** An empty Metadata instance, readonly. */
    @NotNull
    @Unmodifiable
    @Contract(pure = true)
    public static Metadata emptyMetadata() {
        return EMPTY_METADATA;
    }

    /** Initializes an empty instance. */
    public Metadata() {
        this.map = new HashMap<>();
    }

    /** Wraps an existing Map into a Metadata object - does not clone the content */
    public Metadata(final @NotNull Map<String, String[]> map) {
        this(map, false);
    }

    /**
     * Wraps an existing HashMap into a Metadata object if {@code clone} is false. Otherwise it
     * creates a deep copy of the map.
     */
    public Metadata(final @NotNull Map<String, String[]> map, final boolean clone) {
        Objects.requireNonNull(map);
        if (clone) {
            this.map =
                    map.entrySet().stream()
                            .collect(Collectors.toMap(Entry::getKey, e -> e.getValue().clone()));
        } else {
            this.map = map;
        }
    }

    /** Returns true if this instance is locked by the serializer. */
    public boolean isLocked() {
        return locked;
    }

    /** Returns true if the instance has a {@link Collections#EMPTY_MAP} as backing map. */
    public boolean isReadOnly() {
        return this.map == EMPTY_MAP;
    }

    /** Returns the first value for the key or null if it does not exist */
    @Contract(pure = true)
    @Nullable
    public String getFirstValue(@NotNull String key) {
        String[] values = map.get(key);
        if (values == null || values.length == 0) return null;
        return values[0];
    }

    /** Returns the first value for the key or null if it does not exist, given a prefix */
    @Contract(pure = true)
    @Nullable
    public String getFirstValue(@NotNull String key, @Nullable String prefix) {
        if (isNullOrEmpty(prefix)) return getFirstValue(key);
        return getFirstValue(prefix + key);
    }

    /**
     * Returns the array of values for the given {@code key} and {@code prefix} by calling {@code
     * getValues(prefix+key)} It ignores the prefix if it is null or empty.
     */
    @Contract(pure = true)
    @Nullable
    public String[] getValues(@NotNull String key, @Nullable String prefix) {
        if (isNullOrEmpty(prefix)) return getValues(key);
        return getValues(prefix + key);
    }

    /** Returns the array of values for the given {@code key}. */
    @Contract(pure = true)
    @Nullable
    public String[] getValues(@NotNull String key) {
        String[] values = map.get(key);
        if (values == null || values.length == 0) return null;
        return values;
    }

    /**
     * Returns true if this Metadata contains the given {@code key}.
     *
     * @throws NullPointerException if the {@code key} is null.
     */
    @Contract(pure = true)
    public boolean containsKey(@NotNull String key) {
        return map.containsKey(key);
    }

    /** Returns true if the {@code key} points to an array containing the provided {@code value}. */
    @Contract(pure = true)
    public boolean containsKeyWithValue(@NotNull String key, @Nullable String value) {
        String[] values = getValues(key);
        if (values == null) return false;
        for (String s : values) {
            if (s.equals(value)) return true;
        }
        return false;
    }

    /** Internal set method without safety check. */
    @Contract(mutates = "this")
    @Nullable
    private String[] putValueInternal(@NotNull String key, @Nullable String value) {
        return map.put(key, new String[] {value});
    }

    /** Internal set method without safety check. */
    @Contract(mutates = "this")
    private void putValuesInternal(@NotNull String key, String[] values) {
        if (values == null || values.length == 0) return;
        map.put(key, values.clone());
    }

    /** Internal put method without safety check. */
    @Contract(mutates = "this")
    private void putAllInternal(@NotNull Map<String, String[]> mapToPut) {
        if (mapToPut.isEmpty()) return;

        for (Map.Entry<String, String[]> entry : mapToPut.entrySet()) {
            putValuesInternal(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Puts all the entries of {@code mapToPut} into the current instance, overrides potentially
     * existing keys.
     */
    @Contract(mutates = "this")
    public void putAll(@NotNull Map<String, String[]> mapToPut) {
        checkLockException();
        putAllInternal(mapToPut);
    }

    /**
     * Puts all the entries of {@code mapToPut} into the current instance with the provided prefix,
     * overrides potentially existing keys.
     */
    @Contract(mutates = "this")
    public void putAll(@NotNull Map<String, String[]> mapToPut, @Nullable String prefix) {
        checkLockException();

        if (isNullOrEmpty(prefix)) {
            putAllInternal(mapToPut);
        } else {
            for (Map.Entry<String, String[]> entry : mapToPut.entrySet()) {
                putValuesInternal(prefix + entry.getKey(), entry.getValue());
            }
        }
    }

    /** Puts all the metadata into the current instance, overrides potentially existing keys. */
    @Contract(mutates = "this")
    public void putAll(@NotNull Metadata metadata) {
        putAll(metadata.map);
    }

    /**
     * Puts all prefixed metadata into the current instance, overrides potentially existing keys.
     *
     * @param metadata metadata to be added
     * @param prefix string to prefix keys in metadata before adding them to the current metadata.
     *     No separator is inserted between prefix and original key, so the prefix must include any
     *     separator (eg. a dot)
     */
    @Contract(mutates = "this")
    public void putAll(@NotNull Metadata metadata, @Nullable String prefix) {
        putAll(metadata.map, prefix);
    }

    /**
     * Sets the value for a given key and discards the old value. The new value can be null.
     *
     * @return the previous value associated with key, or null if there was no mapping for key. (A
     *     null return can also indicate that the map previously associated null with key, if the
     *     implementation supports null values.)
     */
    @Contract(mutates = "this")
    @Nullable
    public String[] setValue(@NotNull String key, @Nullable String value) {
        checkLockException();
        return putValueInternal(key, value);
    }

    /** Set the value for a given key. The value can be null. */
    @Contract(mutates = "this")
    public void setValues(@NotNull String key, String... values) {
        checkLockException();
        putValuesInternal(key, values);
    }

    /** Add or set the value for a given key. The value can be null. */
    @Contract(mutates = "this")
    public void addValue(@NotNull String key, @Nullable String value) {
        checkLockException();

        String[] existingValues = map.get(key);
        if (existingValues == null || existingValues.length == 0) {
            putValueInternal(key, value);
        } else {
            map.put(key, concat(existingValues, value));
        }
    }

    /** Add or set the value for a given key. The values can be null. */
    @Contract(mutates = "this")
    public void addValues(@NotNull String key, String... values) {
        checkLockException();

        if (values == null || values.length == 0) return;

        String[] existingValues = map.get(key);
        if (existingValues == null) {
            putValuesInternal(key, values.clone());
        } else {
            map.put(key, concat(existingValues, values));
        }
    }

    /** Add or set the value for a given key. The value can be null. */
    @Contract(mutates = "this")
    public void addValues(@NotNull String key, @Nullable Collection<String> values) {
        String[] toAdd;

        if (values == null || values.isEmpty()) {
            toAdd = null;
        } else {
            toAdd = values.toArray(new String[0]);
        }

        addValues(key, toAdd);
    }

    /** @return the previous value(s) associated with <tt>key</tt> */
    @Contract(mutates = "this")
    @NotNull
    public String[] remove(@NotNull String key) {
        checkLockException();
        return map.remove(key);
    }

    @NotNull
    public String toString() {
        return toString("");
    }

    /** Returns a String representation of the metadata with one K/V per line */
    @NotNull
    public String toString(String prefix) {
        StringBuilder sb = new StringBuilder();
        if (prefix == null) prefix = "";
        for (Entry<String, String[]> entry : map.entrySet()) {
            for (String val : entry.getValue()) {
                sb.append(prefix).append(entry.getKey()).append(": ").append(val).append("\n");
            }
        }
        return sb.toString();
    }

    /** Returns the size of the keys in this */
    public int size() {
        return map.size();
    }

    /** Returns a set of all keys of this instance */
    @NotNull
    public Set<@Nullable String> keySet() {
        return map.keySet();
    }

    /** Returns the first non-empty value found for the keys or null if none found. */
    @Nullable
    public static String getFirstValue(@NotNull Metadata md, String... keys) {
        for (String key : keys) {
            String val = md.getFirstValue(key);
            if (!isNullOrEmpty(val)) return val;
        }
        return null;
    }

    /**
     * Returns the underlying Map
     *
     * @deprecated replace with getMap, getShallowCopyOfMap or getDeepCopyOfMap.
     */
    @Deprecated
    @NotNull
    public Map<String, String[]> asMap() {
        return map;
    }

    /** Returns the underlying map. */
    @NotNull
    public Map<String, String[]> getMap() {
        return map;
    }

    /**
     * Get a deep copy of the underlying map. Changes to the keys and values are not changing the
     * original metadata.
     */
    @NotNull
    public Map<String, String[]> createDeepCopyOfMap() {
        return map.entrySet().stream()
                .collect(Collectors.toMap(Entry::getKey, e -> e.getValue().clone()));
    }

    /**
     * Creates a plain copy of this with the same underlying map. Therefor all changes done to the
     * new instance are also done to the origin metadata
     *
     * @return a copy of this
     */
    @NotNull
    public Metadata copy() {
        return new Metadata(map);
    }

    /**
     * Get a deep copy of this. Changes to the keys and values are not changing the original
     * metadata.
     */
    @NotNull
    public Metadata copyDeep() {
        return new Metadata(createDeepCopyOfMap());
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
    @Contract(" -> this")
    @NotNull
    public Metadata lock() {
        locked = true;
        return this;
    }

    /**
     * Release the lock on a metadata
     *
     * @since 1.16
     */
    @Contract(" -> this")
    @NotNull
    public Metadata unlock() {
        locked = false;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Metadata)) return false;
        Metadata metadata = (Metadata) o;

        for (Entry<String, String[]> stringEntry : map.entrySet()) {
            if (!metadata.map.containsKey(stringEntry.getKey())) {
                return false;
            }
            String[] otherValue = metadata.map.get(stringEntry.getKey());
            if (!Arrays.equals(stringEntry.getValue(), otherValue)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(map);
    }

    /** @since 1.16 */
    private void checkLockException() {
        if (locked)
            throw new ConcurrentModificationException(
                    "Attempt to modify a metadata after it has been sent to the serializer");
    }

    // Easier check.
    @Contract("null -> true")
    private static boolean isNullOrEmpty(@Nullable String s) {
        return s == null || s.isEmpty();
    }

    // use (possibly) intrinsic methods for faster array concat.
    @Contract(pure = true)
    @NotNull
    private static String[] concat(String @NotNull [] arr, @Nullable String toAppend) {
        String[] result = Arrays.copyOf(arr, arr.length + 1);
        result[result.length - 1] = toAppend;
        return result;
    }

    // use (possibly) intrinsic methods for faster array concat.
    @Contract(pure = true)
    @NotNull
    private static String[] concat(String @NotNull [] arr1, String @NotNull [] arr2) {
        String[] result = Arrays.copyOf(arr1, arr1.length + arr2.length);
        System.arraycopy(arr2, 0, result, arr1.length, arr2.length);
        return result;
    }
}
