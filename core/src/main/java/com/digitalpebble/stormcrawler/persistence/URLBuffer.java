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

public class URLBuffer {

    private Set<String> in_buffer = new HashSet<>();
    private Map<String, Queue<URLMetadata>> queues = Collections
            .synchronizedMap(new LinkedHashMap<>());

    /**
     * Returns false if the URL was already in the buffer, true if it wasn't and
     * was added
     **/
    public synchronized boolean add(String URL, Metadata m) {
        if (in_buffer.contains(URL)) {
            return false;
        }
        // determine which queue to use
        // TODO configure with other than hostname
        String key = null;
        try {
            URL u = new URL(URL);
            key = u.getHost();
        } catch (MalformedURLException e) {
            return false;
        }

        // create the queue if it does not exist
        // and add the
        queues.computeIfAbsent(key, k -> new LinkedList<URLMetadata>())
                .add(new URLMetadata(URL, m));
        in_buffer.add(URL);
        return true;
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

        // remove it from the list of URLs in the queue
        in_buffer.remove(item.url);
        return new Values(item.url, item.metadata);
    }

    public synchronized boolean hasNext() {
        return queues.isEmpty();
    }

    private class URLMetadata {
        String url;
        Metadata metadata;

        URLMetadata(String u, Metadata m) {
            url = u;
            metadata = m;
        }
    }

}
