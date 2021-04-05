package com.digitalpebble.stormcrawler.filtering;

import java.net.URL;

import com.digitalpebble.stormcrawler.Metadata;

public class URLFilterDecorator implements URLFilter {
    private URLFilter wrappee;

    public URLFilterDecorator(URLFilter wrappee) {
        this.wrappee = wrappee;
    }
    
    @Override
    public String filter(URL sourceUrl, Metadata sourceMetadata, String urlToFilter) {
        return wrappee.filter(sourceUrl, sourceMetadata, urlToFilter);
    }
}
