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

import static java.util.Collections.EMPTY_MAP;

import com.digitalpebble.stormcrawler.Metadata;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.DefaultArraySerializers;
import com.esotericsoftware.kryo.serializers.DefaultSerializers;
import java.util.Map;
import org.apache.storm.serialization.types.HashMapSerializer;

/** A Kryo serializer for Metadata */
// We need that serializer to make sure, that we do protect an empty metadata instance.
// Otherwise it'll be converted to an HashMap.
public class MetadataKryoSerializer extends Serializer<Metadata> {

    private final HashMapSerializer hashMapSerializer = new HashMapSerializer();

    public MetadataKryoSerializer() {
        hashMapSerializer.setKeysCanBeNull(false);
        hashMapSerializer.setKeysCanBeNull(true);
        hashMapSerializer.setKeyClass(String.class, new DefaultSerializers.StringSerializer());
        hashMapSerializer.setValueClass(
                String[].class, new DefaultArraySerializers.StringArraySerializer());
    }

    @Override
    public void write(Kryo kryo, Output output, Metadata object) {
        boolean isEmpty = object.getMap() == EMPTY_MAP;
        output.writeBoolean(isEmpty);
        if (!isEmpty) {
            hashMapSerializer.write(kryo, output, object.getMap());
        }
    }

    @Override
    public Metadata read(Kryo kryo, Input input, Class<Metadata> type) {
        boolean isEmpty = input.readBoolean();
        if (isEmpty) {
            return Metadata.EMPTY_METADATA;
        } else {
            //noinspection unchecked
            return new Metadata(
                    (Map<String, String[]>) hashMapSerializer.read(kryo, input, Map.class));
        }
    }
}
