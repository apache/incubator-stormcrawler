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

import com.digitalpebble.stormcrawler.persistence.Status;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class MetadataTest {

    @Test
    public void testShallowCopy() {
        final Metadata meta = new Metadata();
        meta.setValue("a", "a");
        meta.setValue("b", "b");
        final Metadata copy = meta.copy();
        copy.addValue("b", "c");
        copy.setValues("u", "u", "v", "w");

        Assert.assertTrue(meta.containsKey("a"));
        Assert.assertTrue(copy.containsKey("a"));
        Assert.assertArrayEquals(meta.getValues("b"), copy.getValues("b"));

        Assert.assertTrue(meta.containsKey("u"));
        Assert.assertTrue(copy.containsKey("u"));
        Assert.assertArrayEquals(meta.getValues("u"), copy.getValues("u"));
    }

    @Test
    public void testDeepCopy() {
        final Metadata meta = new Metadata();
        meta.setValue("a", "a");
        final Metadata copy = meta.copyDeep();
        copy.setValues("u", "u", "v", "w");
        Assert.assertTrue(meta.containsKey("a"));
        Assert.assertTrue(copy.containsKey("a"));
        Assert.assertFalse(meta.containsKey("u"));
        Assert.assertTrue(copy.containsKey("u"));
    }

    @Test
    public void testGetValue_prefix() {
        final Metadata meta = new Metadata();
        meta.setValue("a.b", "a");
        Assert.assertNotNull(meta.getFirstValue("b", "a."));
    }

    @Test
    public void testAddValue_newKey() {
        final Metadata meta = new Metadata();
        meta.setValue("a", "a");
        meta.setValue("b", "b");
        meta.setValue("c", "c");
        final Map<String, String[]> deepCopyOfMap = meta.createDeepCopyOfMap();
        meta.addValue("d", "d");

        Assert.assertNotEquals(deepCopyOfMap.size(), meta.size());
        Assert.assertEquals(deepCopyOfMap.size() + 1, meta.size());
        for (Map.Entry<String, String[]> x : deepCopyOfMap.entrySet()) {
            Assert.assertTrue(meta.containsKey(x.getKey()));
        }
        Assert.assertTrue(meta.containsKey("d"));
    }

    @Test
    public void testAddValue_oldKey() {
        final Metadata meta = new Metadata();
        meta.setValue("a", "a");
        meta.setValue("b", "b");
        meta.setValue("c", "c");
        final Map<String, String[]> deepCopyOfMap = meta.createDeepCopyOfMap();
        meta.addValue("c", "d");

        Assert.assertEquals(deepCopyOfMap.size(), meta.size());
        for (Map.Entry<String, String[]> x : deepCopyOfMap.entrySet()) {
            Assert.assertTrue(meta.containsKey(x.getKey()));
        }
        Assert.assertTrue(meta.containsKeyWithValue("c", "c"));
        Assert.assertTrue(meta.containsKeyWithValue("c", "d"));
    }

    @Test
    public void testAddValues_newKey() {
        final Metadata meta = new Metadata();
        meta.setValue("a", "a");
        meta.setValue("b", "b");
        meta.setValue("c", "c");
        final Map<String, String[]> deepCopyOfMap = meta.createDeepCopyOfMap();
        meta.addValues("d", "d", "e");

        Assert.assertNotEquals(deepCopyOfMap.size(), meta.size());
        Assert.assertEquals(deepCopyOfMap.size() + 1, meta.size());
        for (Map.Entry<String, String[]> x : deepCopyOfMap.entrySet()) {
            Assert.assertTrue(meta.containsKey(x.getKey()));
        }
        Assert.assertTrue(meta.containsKey("d"));
        Assert.assertTrue(meta.containsKeyWithValue("d", "d"));
        Assert.assertTrue(meta.containsKeyWithValue("d", "e"));
    }

    @Test
    public void testAddValues_oldKey() {
        final Metadata meta = new Metadata();
        meta.setValue("a", "a");
        meta.setValue("b", "b");
        meta.setValue("c", "c");
        final Map<String, String[]> deepCopyOfMap = meta.createDeepCopyOfMap();
        meta.addValues("c", "d", "e");

        Assert.assertEquals(deepCopyOfMap.size(), meta.size());
        for (Map.Entry<String, String[]> x : deepCopyOfMap.entrySet()) {
            Assert.assertTrue(meta.containsKey(x.getKey()));
        }
        Assert.assertTrue(meta.containsKeyWithValue("c", "c"));
        Assert.assertTrue(meta.containsKeyWithValue("c", "d"));
        Assert.assertTrue(meta.containsKeyWithValue("c", "e"));
    }

    @Test
    public void testPutValues_oldKey() {
        final Metadata meta = new Metadata();
        meta.setValue("a", "a");
        meta.setValue("b", "b");
        meta.setValue("c", "c");
        final Map<String, String[]> deepCopyOfMap = meta.createDeepCopyOfMap();

        final HashMap<String, String[]> stringHashMap = new HashMap<>();

        stringHashMap.put("a", new String[] {"ak", "av"});
        stringHashMap.put("b", new String[] {"bk", "bv"});
        meta.putAll(stringHashMap);

        Assert.assertEquals(deepCopyOfMap.size(), meta.size());
        for (Map.Entry<String, String[]> x : deepCopyOfMap.entrySet()) {
            Assert.assertTrue(meta.containsKey(x.getKey()));
        }
        Assert.assertTrue(meta.containsKeyWithValue("a", "ak"));
        Assert.assertTrue(meta.containsKeyWithValue("a", "av"));
        Assert.assertTrue(meta.containsKeyWithValue("b", "bk"));
        Assert.assertTrue(meta.containsKeyWithValue("b", "bv"));
    }

    @Test
    public void testPutValues_newKey() {
        final Metadata meta = new Metadata();
        meta.setValue("a", "a");
        meta.setValue("b", "b");
        meta.setValue("c", "c");
        final Map<String, String[]> deepCopyOfMap = meta.createDeepCopyOfMap();

        final HashMap<String, String[]> stringHashMap = new HashMap<>();

        stringHashMap.put("d", new String[] {"k", "v"});
        meta.putAll(stringHashMap);

        Assert.assertEquals(deepCopyOfMap.size() + 1, meta.size());
        for (Map.Entry<String, String[]> x : deepCopyOfMap.entrySet()) {
            Assert.assertTrue(meta.containsKey(x.getKey()));
        }
        Assert.assertTrue(meta.containsKeyWithValue("d", "k"));
        Assert.assertTrue(meta.containsKeyWithValue("d", "v"));
    }

    @Test
    public void testPutValues_oldKey_prefix() {
        final Metadata meta = new Metadata();
        meta.setValue("a", "a");
        meta.setValue("b", "b");
        meta.setValue("c", "c");
        final Map<String, String[]> deepCopyOfMap = meta.createDeepCopyOfMap();

        final HashMap<String, String[]> stringHashMap = new HashMap<>();

        stringHashMap.put("a", new String[] {"ak", "av"});
        stringHashMap.put("b", new String[] {"bk", "bv"});
        meta.putAll(stringHashMap, "test.");

        Assert.assertEquals(deepCopyOfMap.size() + 2, meta.size());
        for (Map.Entry<String, String[]> x : deepCopyOfMap.entrySet()) {
            Assert.assertTrue(meta.containsKey(x.getKey()));
        }
        Assert.assertFalse(meta.containsKeyWithValue("a", "ak"));
        Assert.assertFalse(meta.containsKeyWithValue("a", "av"));
        Assert.assertFalse(meta.containsKeyWithValue("b", "bk"));
        Assert.assertFalse(meta.containsKeyWithValue("b", "bv"));
        Assert.assertTrue(meta.containsKeyWithValue("test.a", "ak"));
        Assert.assertTrue(meta.containsKeyWithValue("test.a", "av"));
        Assert.assertTrue(meta.containsKeyWithValue("test.b", "bk"));
        Assert.assertTrue(meta.containsKeyWithValue("test.b", "bv"));
    }

    @Test
    public void testPutValues_newKey_prefix() {
        final Metadata meta = new Metadata();
        meta.setValue("a", "a");
        meta.setValue("b", "b");
        meta.setValue("c", "c");
        final Map<String, String[]> deepCopyOfMap = meta.createDeepCopyOfMap();

        final HashMap<String, String[]> stringHashMap = new HashMap<>();

        stringHashMap.put("d", new String[] {"k", "v"});
        meta.putAll(stringHashMap, "test.");

        Assert.assertEquals(deepCopyOfMap.size() + 1, meta.size());
        for (Map.Entry<String, String[]> x : deepCopyOfMap.entrySet()) {
            Assert.assertTrue(meta.containsKey(x.getKey()));
        }
        Assert.assertFalse(meta.containsKey("d"));
        Assert.assertTrue(meta.containsKey("test.d"));
        Assert.assertTrue(meta.containsKeyWithValue("test.d", "k"));
        Assert.assertTrue(meta.containsKeyWithValue("test.d", "v"));
    }

    @Test
    public void testSerialisation() {

        final Metadata meta = new Metadata();
        meta.setValue("a", "a");
        meta.setValue("b", "b");
        meta.setValue("c", "c");
        meta.setValues("d", "d", "e");
        final Map<String, String[]> deepCopyOfMap = meta.createDeepCopyOfMap();

        final Kryo kryo = new Kryo();
        kryo.register(Metadata.class);
        kryo.register(Status.class);

        byte[] serialized;
        try (ByteArrayOutputStream bOut = new ByteArrayOutputStream()) {
            try (Output output = new Output(bOut)) {
                kryo.writeObject(output, meta);
            }
            serialized = bOut.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Metadata metaNew;
        try (ByteArrayInputStream bIn = new ByteArrayInputStream(serialized)) {
            try (Input input = new Input(bIn)) {
                metaNew = kryo.readObject(input, Metadata.class);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Assert.assertEquals(meta, metaNew);
    }

    @Test
    public void testSerialisationOfEmpty() {

        final Metadata meta = Metadata.emptyMetadata();

        final Kryo kryo = new Kryo();
        kryo.register(Metadata.class);
        kryo.register(Status.class);

        byte[] serialized;
        try (ByteArrayOutputStream bOut = new ByteArrayOutputStream()) {
            try (Output output = new Output(bOut)) {
                kryo.writeObject(output, meta);
            }
            serialized = bOut.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Metadata metaNew;
        try (ByteArrayInputStream bIn = new ByteArrayInputStream(serialized)) {
            try (Input input = new Input(bIn)) {
                metaNew = kryo.readObject(input, Metadata.class);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Assert.assertSame(meta.getMap(), metaNew.getMap());
        Assert.assertSame(meta.getMap(), EMPTY_MAP);
        Assert.assertSame(metaNew.getMap(), EMPTY_MAP);
    }
}
