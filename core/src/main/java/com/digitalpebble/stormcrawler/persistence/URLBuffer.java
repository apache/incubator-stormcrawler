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

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.tuple.Values;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.util.ConfUtils;

/**
 * Buffers URLs to be processed into separate queues; used by spouts. Guarantees
 * that no URL can be put in the buffer more than once.
 * 
 * Configured by setting
 * 
 * urlbuffer.class: "com.digitalpebble.stormcrawler.persistence.SimpleURLBuffer"
 * 
 * in the configuration
 * 
 * @since 1.15
 **/

public interface URLBuffer {

    /**
     * Class to use for Scheduler. Must extend the class Scheduler.
     */
    public static final String bufferClassParamName = "urlbuffer.class";

    /**
     * Stores the URL and its Metadata under a given key.
     * 
     * Implementations of this method should be synchronised
     * 
     * @return false if the URL was already in the buffer, true if it wasn't and
     *         was added
     **/
    public abstract boolean add(String URL, Metadata m, String key);

    /**
     * Stores the URL and its Metadata using the hostname as key.
     * 
     * Implementations of this method should be synchronised
     * 
     * @return false if the URL was already in the buffer, true if it wasn't and
     *         was added
     **/
    public default boolean add(String URL, Metadata m) {
        return add(URL, m, null);
    }

    /** Total number of URLs in the buffer **/
    public abstract int size();

    /** Total number of queues in the buffer **/
    public abstract int numQueues();

    /**
     * Retrieves the next available URL, guarantees that the URLs are always
     * perfectly shuffled
     * 
     * Implementations of this method should be synchronised
     * 
     **/
    public abstract Values next();

    /**
     * Implementations of this method should be synchronised
     **/
    public abstract boolean hasNext();

    public abstract void setEmptyQueueListener(EmptyQueueListener l);

    /**
     * Notify the buffer that a URL has been successfully processed used e.g to
     * compute an ideal delay for a host queue
     **/
    public default void acked(String url) {
        // do nothing with the information about URLs being acked
    }

    public default void configure(Map stormConf) {
    }
    
    /** Returns a URLBuffer instance based on the configuration **/
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static URLBuffer getInstance(Map stormConf) {
        URLBuffer buffer;

        String className = ConfUtils.getString(stormConf, bufferClassParamName);

        if (StringUtils.isBlank(className)) {
            throw new RuntimeException(
                    "Missing value for config  " + bufferClassParamName);
        }

        try {
            Class<?> bufferclass = Class.forName(className);
            boolean interfaceOK = URLBuffer.class.isAssignableFrom(bufferclass);
            if (!interfaceOK) {
                throw new RuntimeException(
                        "Class " + className + " must extend URLBuffer");
            }
            buffer = (URLBuffer) bufferclass.newInstance();
            buffer.configure(stormConf);
        } catch (Exception e) {
            throw new RuntimeException("Can't instanciate " + className);
        }

        return buffer;
    }

}