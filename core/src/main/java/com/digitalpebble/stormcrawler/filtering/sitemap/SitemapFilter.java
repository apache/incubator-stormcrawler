/**
 * Licensed to DigitalPebble Ltd under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership.
 * DigitalPebble licenses this file to You under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.digitalpebble.stormcrawler.filtering.sitemap;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.bolt.SiteMapParserBolt;
import com.digitalpebble.stormcrawler.filtering.URLFilter;
import java.net.URL;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * URLFilter which discards URLs discovered in a page which is not a sitemap when sitemaps have been
 * found for that site. This allows to restrict the crawl to pages found in the sitemaps but won't
 * affect sites which do not have sitemaps.
 *
 * <pre>
 *  {
 *    "class": "com.digitalpebble.stormcrawler.filtering.sitemap.SitemapFilter",
 *    "name": "SitemapFilter"
 *  }
 * </pre>
 *
 * Will be replaced by <a href=
 * "https://github.com/DigitalPebble/storm-crawler/issues/711">MetadataFilter to filter based on
 * multiple key values</a>
 *
 * @since 1.14
 */
public class SitemapFilter extends URLFilter {

    @Override
    public @Nullable String filter(
            @Nullable URL sourceUrl,
            @Nullable Metadata sourceMetadata,
            @NotNull String urlToFilter) {

        if (sourceMetadata != null
                && !Boolean.parseBoolean(
                        sourceMetadata.getFirstValue(SiteMapParserBolt.isSitemapKey))
                && Boolean.parseBoolean(
                        sourceMetadata.getFirstValue(SiteMapParserBolt.foundSitemapKey))) {
            return null;
        }
        return urlToFilter;
    }
}
