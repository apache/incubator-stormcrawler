package com.digitalpebble.stormcrawler.persistence;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.tuple.Values;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.util.ConfUtils;

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
    public abstract boolean add(String URL, Metadata m);

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

    /** Returns a IURLBuffer instance based on the configuration **/
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
        } catch (Exception e) {
            throw new RuntimeException("Can't instanciate " + className);
        }

        return buffer;
    }

}