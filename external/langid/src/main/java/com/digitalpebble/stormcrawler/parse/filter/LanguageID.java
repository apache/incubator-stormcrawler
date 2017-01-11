/**
 * Licensed to DigitalPebble Ltd under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * DigitalPebble licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.digitalpebble.stormcrawler.parse.filter;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.w3c.dom.DocumentFragment;

import com.digitalpebble.stormcrawler.parse.ParseFilter;
import com.digitalpebble.stormcrawler.parse.ParseResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Optional;
import com.optimaize.langdetect.LanguageDetector;
import com.optimaize.langdetect.LanguageDetectorBuilder;
import com.optimaize.langdetect.i18n.LdLocale;
import com.optimaize.langdetect.ngram.NgramExtractors;
import com.optimaize.langdetect.profiles.LanguageProfile;
import com.optimaize.langdetect.profiles.LanguageProfileReader;
import com.optimaize.langdetect.text.CommonTextObjectFactories;
import com.optimaize.langdetect.text.TextObject;
import com.optimaize.langdetect.text.TextObjectFactory;

/**
 * Language identification; the language code gets stored in the metadata. <br>
 * 
 * To use it, just add the module as a dependency in your pom and include
 * 
 * ```json { "class": "com.digitalpebble.stormcrawler.parse.filter.LanguageID",
 * "name": "LanguageID", "params": { "key": "lang" } }
 * 
 * in the parse filter config. ```
 **/

public class LanguageID extends ParseFilter {

    private static LanguageDetector languageDetector;

    private static final TextObjectFactory textObjectFactory = CommonTextObjectFactories
            .forDetectingOnLargeText();

    private String mdKey = "lang";

    static {
        try {
            // load all languages:
            List<LanguageProfile> languageProfiles = new LanguageProfileReader()
                    .readAllBuiltIn();
            // build language detector:
            languageDetector = LanguageDetectorBuilder
                    .create(NgramExtractors.standard())
                    .withProfiles(languageProfiles).build();
        } catch (IOException e) {
            throw new RuntimeException("Error while loading language profiles",
                    e);
        }
    }

    @Override
    public void configure(Map stormConf, JsonNode filterParams) {

        JsonNode node = filterParams.get("key");
        if (node != null && node.isTextual()) {
            mdKey = node.asText("lang");
        }
    }

    @Override
    public void filter(String url, byte[] content, DocumentFragment doc,
            ParseResult parse) {

        String text = parse.get(url).getText();
        if (StringUtils.isBlank(text)) {
            return;
        }

        TextObject textObject = textObjectFactory.forText(text);
        synchronized (languageDetector) {
            Optional<LdLocale> lang = languageDetector.detect(textObject);
            if (lang.isPresent()) {
                String code = lang.get().getLanguage();
                parse.get(url).getMetadata().setValue(mdKey, code);
            }
        }
    }

}
