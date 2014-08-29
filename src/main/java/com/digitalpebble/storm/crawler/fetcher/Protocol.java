package com.digitalpebble.storm.crawler.fetcher;

import backtype.storm.Config;

public interface Protocol {

    public ProtocolResponse getProtocolOutput(String url) throws Exception;

    public void configure(Config conf);
}
