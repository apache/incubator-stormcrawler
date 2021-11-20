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
package com.digitalpebble.stormcrawler.parse.filter;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.parse.ParseFilter;
import com.digitalpebble.stormcrawler.parse.ParseResult;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.w3c.dom.DocumentFragment;

/**
 * Normalises the MimeType value e.g. text/html; charset=UTF-8 => HTML application/pdf => PDF and
 * creates a new entry with a key 'format' in the metadata. Requires the JSoupParserBolt to be used
 * with the configuration _detect.mimetype_ set to true.
 */
public class MimeTypeNormalization extends ParseFilter {

    @Override
    public void filter(String url, byte[] content, DocumentFragment doc, ParseResult parse) {

        Metadata m = parse.get(url).getMetadata();
        String ct = m.getFirstValue("parse.Content-Type");
        if (StringUtils.isBlank(ct)) {
            ct = "unknown";
        } else if (ct.toLowerCase().contains("html")) {
            ct = "html";
        } else if (ct.toLowerCase().contains("pdf")) {
            ct = "pdf";
        } else if (ct.toLowerCase().contains("word")) {
            ct = "word";
        } else if (ct.toLowerCase().contains("excel")) {
            ct = "excel";
        } else if (ct.toLowerCase().contains("powerpoint")) {
            ct = "powerpoint";
        } else if (ct.toLowerCase().startsWith("video/")) {
            ct = "video";
        } else if (ct.toLowerCase().startsWith("image/")) {
            ct = "image";
        } else if (ct.toLowerCase().startsWith("audio/")) {
            ct = "audio";
        } else {
            ct = "other";
        }
        m.setValue("format", ct);
    }
}
