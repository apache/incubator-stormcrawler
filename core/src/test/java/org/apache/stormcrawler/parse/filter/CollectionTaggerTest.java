/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.stormcrawler.parse.filter;

import com.fasterxml.jackson.databind.node.NullNode;
import java.io.IOException;
import java.util.HashMap;
import org.apache.stormcrawler.parse.ParseResult;
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
