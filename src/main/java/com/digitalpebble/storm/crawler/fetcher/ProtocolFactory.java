package com.digitalpebble.storm.crawler.fetcher;

import java.net.URL;
import java.util.WeakHashMap;

import com.digitalpebble.storm.crawler.fetcher.protocol.http.HttpProtocol;

import backtype.storm.Config;

public class ProtocolFactory {

    private final Config config;

    private final WeakHashMap<String, Protocol> cache = new WeakHashMap<String, Protocol>();

    public ProtocolFactory(Config conf) {
        config = conf;
    }

    /** Returns an instance of the protocol to use for a given URL **/
    public synchronized Protocol getProtocol(URL url) {
        // get the protocol
        String protocol = url.getProtocol();
        Protocol pp = cache.get(protocol);
        if (pp != null)
            return pp;

        // yuk! hardcoded for now
        pp = new HttpProtocol();
        pp.configure(config);
        cache.put(protocol, pp);
        return pp;
    }

}
