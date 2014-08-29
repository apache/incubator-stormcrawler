package com.digitalpebble.storm.crawler.fetcher;

import java.util.HashMap;

public class ProtocolResponse {

    final byte[] content;
    final int statusCode;
    final HashMap<String, String[]> metadata;

    public ProtocolResponse(byte[] c, int s, HashMap<String, String[]> md) {
        content = c;
        statusCode = s;
        metadata = md;
    }

    public byte[] getContent() {
        return content;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public HashMap<String, String[]> getMetadata() {
        return metadata;
    }

}
