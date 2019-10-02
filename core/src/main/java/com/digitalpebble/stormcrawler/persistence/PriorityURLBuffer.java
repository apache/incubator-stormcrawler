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

import java.time.Instant;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

/**
 * Checks how long the previous URLs took
 **/

public class PriorityURLBuffer extends AbstractURLBuffer
        implements RemovalListener<String, Object[]> {

    static final Logger LOG = LoggerFactory.getLogger(PriorityURLBuffer.class);

    static private int MAXTIMEMSEC = 60000;

    // keeps track of the URL having been sent
    private Cache<String, Object[]> urlCache = CacheBuilder.newBuilder()
            .expireAfterWrite(MAXTIMEMSEC, TimeUnit.MILLISECONDS)
            .removalListener(this).build();

    /**
     * Retrieves the next available URL, guarantees that the URLs are always
     * perfectly shuffled
     * 
     * @return null if no entries are available
     **/
    public synchronized Values next() {
        Iterator<Entry<String, Queue<URLMetadata>>> i = queues.entrySet()
                .iterator();

        while (true) {

            if (!i.hasNext()) {
                LOG.trace("Empty iterator");
                return null;
            }

            Map.Entry<String, Queue<URLMetadata>> nextEntry = i.next();

            Queue<URLMetadata> queue = nextEntry.getValue();
            String queueName = nextEntry.getKey();

            // remove the entry, gets added back later
            i.remove();

            LOG.trace("Next queue {}", queueName);

            // is this queue ready to be processed?
            boolean canRelease = ((ScheduledQueue) queue).canRelease();

            URLMetadata item = null;

            if (canRelease) {
                // try the first element
                item = queue.poll();
                LOG.trace("Item {}", item.url);
            } else {
                LOG.trace("Queue {} not ready to release yet", queueName);
            }

            // any left? add to the end of the iterator
            if (!queue.isEmpty()) {
                LOG.debug("adding to the back of the queue {}", queueName);
                queues.put(queueName, queue);
            }
            // notify that the queue is empty
            else {
                if (listener != null) {
                    listener.emptyQueue(queueName);
                }
            }

            if (item != null) {
                ((ScheduledQueue) queue).setLastReleased();
                urlCache.put(item.url,
                        new Object[] { Instant.now(), queueName });
                // remove it from the list of URLs in the queue
                in_buffer.remove(item.url);
                return new Values(item.url, item.metadata);
            }
        }
    }

    /**
     * Takes the average of the last N URLs to determine whether this queue
     * should emit a URL or not
     **/

    class ScheduledQueue extends LinkedList<URLMetadata> {

        private Instant lastRelease;

        // TODO configure size history
        private final int minHistorySize = 5;

        private LinkedList<Long> times = new LinkedList<>();

        void setLastReleased() {
            lastRelease = Instant.now();
        }

        void addTiming(long t) {
            times.addFirst(t);
        }

        boolean canRelease() {
            // return true if enough time has expired since the previous release
            // given the past performance of the last N urls

            int historySize = times.size();

            // not enough history yet? just say yes
            if (historySize < minHistorySize)
                return true;

            // get the average duration over the recent history
            long totalMsec = 0l;
            for (Long t : times) {
                totalMsec += t;
            }
            long average = totalMsec / historySize;

            // trim the history to the last N items
            times = (LinkedList<Long>) times.subList(0, minHistorySize);

            // check that enough time has elapsed
            // since the previous release from this queue
            return lastRelease.plusMillis(average).isBefore(Instant.now());
        }
    }

    public void acked(String url) {
        // get notified that the URL has been acked
        // use that to compute how long it took
        Object[] cached = (Object[]) urlCache.getIfPresent(url);
        // has already been discarded
        if (cached == null) {
            return;
        }

        Instant t = (Instant) cached[0];
        String key = (String) cached[1];

        long tookmsec = Instant.now().toEpochMilli() - t.toEpochMilli();

        // get the queue and add the timing

        ScheduledQueue queue = (ScheduledQueue) queues.get(key);

        // TODO what if it does not exist
        if (queue != null)
            queue.addTiming(tookmsec);
    }

    @Override
    protected Queue<URLMetadata> getQueueInstance() {
        return new ScheduledQueue();
    }

    @Override
    public void onRemoval(RemovalNotification<String, Object[]> notification) {

        String key = (String) notification.getValue()[1];

        // get the queue and add the max timing
        ScheduledQueue queue = (ScheduledQueue) queues.get(key);

        // TODO what if it does not exist
        if (queue != null)
            queue.addTiming(MAXTIMEMSEC);
    }

}
