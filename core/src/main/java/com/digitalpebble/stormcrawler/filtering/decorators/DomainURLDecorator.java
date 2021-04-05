package com.digitalpebble.stormcrawler.filtering.decorators;

import java.net.URL;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.filtering.URLFilter;
import com.digitalpebble.stormcrawler.filtering.URLFilterDecorator;

public class DomainURLDecorator extends URLFilterDecorator {
    private String regex;

    public DomainURLDecorator(URLFilter wrappee, String domain) {
        super(wrappee);
        regex = "\\." + domain + "\\/?";
    }

    @Override
    public String filter(URL sourceUrl, Metadata sourceMetadata, String urlToFilter) {
        if (urlToFilter == null || !urlToFilter.matches(regex)) {
            return null;
        }
        
        return super.filter(sourceUrl, sourceMetadata, urlToFilter);
    }
}
