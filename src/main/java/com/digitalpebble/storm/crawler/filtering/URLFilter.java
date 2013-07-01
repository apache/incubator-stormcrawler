package com.digitalpebble.storm.crawler.filtering;

import org.codehaus.jackson.JsonNode;

/**
 * Unlike Nutch, URLFilters can normalise the URLs as well as filtering them.
 * URLFilter instances should be used via URLFilters
 **/
public interface URLFilter {

    /**
     * Returns null if the URL is to be removed or a normalised representation
     * which can correspond to the input URL
     **/
    public String filter(String URL);

    /** Configuration of the filter with a JSONNode object **/
    public void configure(JsonNode jsonNode);

}
