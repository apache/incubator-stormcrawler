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

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;

import org.apache.storm.tuple.Values;

import com.digitalpebble.stormcrawler.Metadata;

/** 
 * Buffers URLs to be processed into separate queues; used by spouts.
 * Guarantees that no URL can be put in the buffer more than once.
 * @since 1.15
 **/

public class URLBuffer {

    private Set<String> in_buffer = new HashSet<>();
    private Map<String, Queue<URLMetadata>> queues = Collections
            .synchronizedMap(new LinkedHashMap<>());
    private EmptyQueueListener listener = null;

    /**
     * Stores the URL and its Metadata under a given key.
     * 
     * @return false if the URL was already in the buffer, true if it wasn't and
     *         was added
     **/
    public synchronized boolean add(String URL, Metadata m, String key) {
        if (in_buffer.contains(URL)) {
            return false;
        }

        // determine which queue to use
        // configure with other than hostname
        if (key == null) {
            try {
                URL u = new URL(URL);
                key = u.getHost();
            } catch (MalformedURLException e) {
                return false;
            }
        }

        // create the queue if it does not exist
        // and add the url
        queues.computeIfAbsent(key, k -> new LinkedList<URLMetadata>())
                .add(new URLMetadata(URL, m));
        return in_buffer.add(URL);
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

    /**
     * Retrieves the next available URL, guarantees that the URLs are always
     * perfectly shuffled
     **/
    public synchronized Values next() {
        Iterator<Entry<String, Queue<URLMetadata>>> i = queues.entrySet()
                .iterator();

        if (!i.hasNext()) {
            return null;
        }

        Map.Entry<String, Queue<URLMetadata>> nextEntry = i.next();

        Queue<URLMetadata> queue = nextEntry.getValue();
        String queueName = nextEntry.getKey();

        // remove the entry
        i.remove();

        // remove the first element
        URLMetadata item = queue.poll();

        // any left? add to the end of the iterator
        if (!queue.isEmpty()) {
            queues.put(queueName, queue);
        }
        // notify that the queue is empty
        else {
            if (listener != null) {
                listener.emptyQueue(queueName);
            }
        }

        // remove it from the list of URLs in the queue
        in_buffer.remove(item.url);
        return new Values(item.url, item.metadata);
    }

    public synchronized boolean hasNext() {
        return !queues.isEmpty();
    }

    private class URLMetadata {
        String url;
        Metadata metadata;

        URLMetadata(String u, Metadata m) {
            url = u;
            metadata = m;
        }
    }

    public void setEmptyQueueListener(EmptyQueueListener l) {
        listener = l;
    }
    
}
