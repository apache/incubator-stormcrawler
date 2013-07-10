package com.digitalpebble.storm.crawler.fetcher;

import com.digitalpebble.storm.crawler.util.Configuration;

public interface Protocol {

    public ProtocolResponse getProtocolOutput(String url) throws Exception;
    
    public void configure(Configuration conf);
}
