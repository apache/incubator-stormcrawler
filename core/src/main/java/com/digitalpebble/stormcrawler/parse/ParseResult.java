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

package com.digitalpebble.stormcrawler.parse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.digitalpebble.stormcrawler.Metadata;

public class ParseResult implements Iterable<Map.Entry<String, ParseData>> {

    private List<Outlink> outlinks;
    private Map<String, ParseData> parseMap;

    public ParseResult() {
        this(new HashMap<String, ParseData>(), new ArrayList<>());
    }

    public ParseResult(List<Outlink> links) {
        this(new HashMap<String, ParseData>(), links);
    }

    public ParseResult(Map<String, ParseData> map) {
        this(map, new ArrayList<>());
    }

    public ParseResult(Map<String, ParseData> map, List<Outlink> links) {
        if (map == null) {
            throw new NullPointerException();
        }
        parseMap = map;
        outlinks = links;
    }

    public boolean isEmpty() {
        return parseMap.isEmpty();
    }

    public int size() {
        return parseMap.size();
    }

    public List<Outlink> getOutlinks() {
        return outlinks;
    }

    public void setOutlinks(List<Outlink> outlinks) {
        this.outlinks = outlinks;
    }

    /**
     * @return An existent instance of Parse for the given URL or an empty one
     *         if none can be found, useful to avoid unnecessary checks in the
     *         parse plugins
     */
    public ParseData get(String URL) {
        if (parseMap.get(URL) == null) {
            ParseData parse = new ParseData();
            parseMap.put(URL, parse);
            return parse;
        }

        return parseMap.get(URL);
    }

    public String[] getValues(String URL, String key) {
        ParseData parseInfo = parseMap.get(URL);

        if (parseInfo == null) {
            return null;
        }

        return parseInfo.getValues(key);
    }

    public void put(String URL, String key, String value) {
        get(URL).getMetadata().addValue(key, value);
    }

    public void put(String URL, Metadata metadata) {
        get(URL).getMetadata().putAll(metadata);
    }

    public Map<String, ParseData> getParseMap() {
        return parseMap;
    }

    @Override
    public Iterator<Map.Entry<String, ParseData>> iterator() {
        return parseMap.entrySet().iterator();
    }

    @Override
    public String toString() {
        return parseMap.toString();
    }

}
