package com.digitalpebble.stormcrawler.util;

import java.io.IOException;
import java.net.MalformedURLException;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;

import com.digitalpebble.stormcrawler.parse.JSoupDOMBuilder;

public class RefreshTagTest {
    private static final Logger LOG = LoggerFactory.getLogger(RefreshTagTest.class);
    @Test
    public void testExtractRefreshURL() throws MalformedURLException, IOException {	
        String expected = "http://www.example.com/";
	
	String[] htmlStrings = new String[] {
	    "<html><head><META http-equiv=\"refresh\" content=\"0; URL=http://www.example.com/\"></head><body>Lorem ipsum.</body></html>",
	    "<html><head><META http-equiv=\"refresh\" content=\"0;URL=http://www.example.com/\"></head><body>Lorem ipsum.</body></html>",				
	};

	for (String htmlString : htmlStrings) {
	    Document doc = Jsoup.parseBodyFragment(htmlString);
	    DocumentFragment fragment = JSoupDOMBuilder.jsoup2HTML(doc);
	    String redirection = RefreshTag.extractRefreshURL(fragment);
	    LOG.debug("Redirection is {} from {} ", redirection, htmlString);
	    Assert.assertEquals(expected, redirection);
	}
    }
}
