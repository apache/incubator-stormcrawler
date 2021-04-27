package com.digitalpebble.stormcrawler.protocol.okhttp;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;

import org.slf4j.LoggerFactory;

import okhttp3.Call;
import okhttp3.EventListener;

public class DNSResolutionListener extends EventListener {

    private static final org.slf4j.Logger LOG = LoggerFactory
            .getLogger(DNSResolutionListener.class);

    private long dnsStartMillis;

    final Map<String, Long> times;

    public DNSResolutionListener(final Map<String, Long> times) {
        this.times = times;
    }

    @Override
    public void dnsEnd(Call call, String domainName,
            List<InetAddress> inetAddressList) {
        long timeSpent = System.currentTimeMillis() - dnsStartMillis;
        LOG.debug("DNS resolution for {} took {} millisecs", domainName,
                timeSpent);
        times.put(call.toString(), timeSpent);
    }

    @Override
    public void dnsStart(Call call, String domainName) {
        dnsStartMillis = System.currentTimeMillis();
    }

}