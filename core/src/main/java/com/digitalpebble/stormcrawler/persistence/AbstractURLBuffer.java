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

package com.digitalpebble.stormcrawler.persistence;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.util.URLPartitioner;

/**
 * Abstract class for URLBuffer interface, meant to simplify the code of the
 * implementations and provide some default methods
 * 
 * @since 1.15
 **/
public abstract class AbstractURLBuffer implements URLBuffer {

    protected Set<String> in_buffer = new HashSet<>();
    protected EmptyQueueListener listener = null;

    protected final URLPartitioner partitioner = new URLPartitioner();

    public void configure(Map stormConf) {
        partitioner.configure(stormConf);
    }

    /**
     * Stores the URL and its Metadata using the hostname as key.
     * 
     * @return false if the URL was already in the buffer, true if it wasn't and
     *         was added
     **/
    public synchronized boolean add(String URL, Metadata m) {
        return add(URL, m, null);
    }

    /** Total number of URLs in the buffer **/
    public int size() {
        return in_buffer.size();
    }

    public void setEmptyQueueListener(EmptyQueueListener l) {
        listener = l;
    }
}