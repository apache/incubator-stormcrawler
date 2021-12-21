package com.digitalpebble.stormcrawler.parse.filter;

import com.digitalpebble.stormcrawler.parse.ParseResult;
import com.fasterxml.jackson.databind.node.NullNode;
import java.io.IOException;
import java.util.HashMap;
import org.junit.Assert;
import org.junit.Test;

public class CollectionTaggerTest {

    @Test
    public void testCollectionTagger() throws IOException {

        CollectionTagger tagger = new CollectionTagger();
        tagger.configure(new HashMap<>(), NullNode.instance);
        ParseResult parse = new ParseResult();
        String URL = "http://stormcrawler.net/";
        tagger.filter(URL, null, null, parse);
        String[] collections = parse.get(URL).getMetadata().getValues("collections");
        Assert.assertNotNull(collections);
        Assert.assertEquals(2, collections.length);

        URL = "http://baby.com/tiny-crawler/";
        tagger.filter(URL, null, null, parse);
        collections = parse.get(URL).getMetadata().getValues("collections");
        Assert.assertNull(collections);

        URL = "http://nutch.apache.org/";
        tagger.filter(URL, null, null, parse);
        collections = parse.get(URL).getMetadata().getValues("collections");
        Assert.assertNotNull(collections);
        Assert.assertEquals(1, collections.length);
    }
}
