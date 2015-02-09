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

import java.nio.charset.StandardCharsets;
import java.util.List;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.digitalpebble.storm.crawler.Metadata;
import com.digitalpebble.storm.crawler.persistence.Status;

/**
 * Converts a byte array into URL + metadata
 */
public class StringTabScheme implements Scheme {

    private Status withStatus = null;

    public StringTabScheme() {
        withStatus = null;
    }

    public StringTabScheme(Status status) {
        withStatus = status;
    }

    @Override
    public List<Object> deserialize(byte[] bytes) {
        String input = new String(bytes, StandardCharsets.UTF_8);

        String[] tokens = input.split("\t");
        if (tokens.length < 1)
            return new Values();

        String url = tokens[0];
        Metadata metadata = null;

        for (int i = 1; i < tokens.length; i++) {
            String token = tokens[i];
            // split into key & value
            int firstequals = token.indexOf("=");
            String value = null;
            String key = token;
            if (firstequals != -1) {
                key = token.substring(0, firstequals);
                value = token.substring(firstequals + 1);
            }
            if (metadata == null)
                metadata = new Metadata();
            metadata.addValue(key, value);
        }

        if (withStatus != null)
            return new Values(url, metadata, withStatus);

        return new Values(url, metadata);
    }

    @Override
    public Fields getOutputFields() {
        if (withStatus != null)
            return new Fields("url", "metadata", "status");
        return new Fields("url", "metadata");
    }
}