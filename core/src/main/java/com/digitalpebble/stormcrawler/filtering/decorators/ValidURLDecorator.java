package com.digitalpebble.stormcrawler.filtering.decorators;

import java.net.URL;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.filtering.URLFilter;
import com.digitalpebble.stormcrawler.filtering.URLFilterDecorator;

public class ValidURLDecorator extends URLFilterDecorator {
    public ValidURLDecorator(URLFilter wrappee) {
        super(wrappee);
    }
    
    @Override
    public String filter(URL sourceUrl, Metadata sourceMetadata, String urlToFilter) {
        if (urlToFilter == null) {
            return null;
        }
        
        if (sourceUrl == null) {
            return urlToFilter;
        }

        return super.filter(sourceUrl, sourceMetadata, urlToFilter);
    }
}