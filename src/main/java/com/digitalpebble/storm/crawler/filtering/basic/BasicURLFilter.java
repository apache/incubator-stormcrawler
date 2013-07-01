package com.digitalpebble.storm.crawler.filtering.basic;

import org.codehaus.jackson.JsonNode;

import com.digitalpebble.storm.crawler.filtering.URLFilter;

public class BasicURLFilter implements URLFilter {

    public String filter(String URL) {
        return URL;
    }

    public void configure(JsonNode jsonNode) {

    }

}
