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

package org.apache.stormcrawler.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.stormcrawler.indexing.AbstractIndexerBolt;
import org.junit.jupiter.api.Test;

/**
 * Metadata labels become SQL column names in {@link IndexerBolt}, and with a glob mapping they come
 * from crawled content, so only plain identifiers may pass.
 */
class ColumnNameValidationTest {

    @Test
    void plainIdentifiersAreAccepted() {
        assertTrue(IndexerBolt.isValidColumnName("title"));
        assertTrue(IndexerBolt.isValidColumnName("key_words2"));
        assertTrue(IndexerBolt.isValidColumnName("_internal"));
        assertTrue(IndexerBolt.isValidColumnName("URL"));
    }

    /**
     * Aliases and plain keys are written by the operator and used as they are, whatever MySQL
     * accepts. Glob mappings produce metadata keys and are not among them.
     */
    @Test
    void configuredLabelsAreAliasesAndPlainKeys() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(
                AbstractIndexerBolt.metadata2fieldParamName,
                List.of("title", " keywords[0] ", "parse.description=col$1", "résumé", "parse.*"));

        assertEquals(
                Set.of("title", "keywords", "col$1", "résumé"), IndexerBolt.configuredLabels(conf));
    }

    @Test
    void nothingIsNotAnIdentifier() {
        assertFalse(IndexerBolt.isValidColumnName(null));
        assertFalse(IndexerBolt.isValidColumnName(""));
    }

    /**
     * The dotted shape a glob mapping such as parse.* produces on ordinary pages, plus the shapes a
     * hostile page can mint through a &lt;meta name="..."&gt; attribute.
     */
    @Test
    void keysCrawledContentCanMintAreRejected() {
        String[] labels = {
            "parse.title",
            "a b",
            "a`b",
            "a\"b",
            "a'b",
            "a;b",
            "a-b",
            "dummy`, status=500 -- ",
            "title) VALUES ('http://evil.example/x', 'x') -- ",
            "title); UPDATE content SET status=500; -- ",
            "a),(SELECT CONCAT(user,0x3a,authentication_string) FROM mysql.user LIMIT 1)) -- "
        };
        for (String label : labels) {
            assertFalse(
                    IndexerBolt.isValidColumnName(label),
                    "must not be usable as a column name: " + label);
        }
    }

    /**
     * A rejected label is crawled content and ends up in a log line, so nothing outside printable
     * ASCII may survive. \p{Cntrl} is not enough: it leaves U+0085, U+00A0, U+2028 and U+2029 in
     * place, three of which break the line for a log reader.
     */
    @Test
    void labelsAreRenderedAsPrintableAsciiForLogging() {
        int[] separators = {0x0A, 0x0D, 0x09, 0x00, 0x0B, 0x85, 0xA0, 0x2028, 0x2029};
        StringBuilder sb = new StringBuilder("evil");
        for (int cp : separators) {
            sb.appendCodePoint(cp).append("X");
        }

        String rendered = IndexerBolt.forLogging(sb.toString());

        assertEquals(
                0,
                rendered.codePoints().filter(c -> c < 0x20 || c > 0x7E).count(),
                "nothing outside printable ASCII may reach the log: " + rendered);
    }

    @Test
    void ordinaryLabelsAreLoggedUnchanged() {
        assertEquals("parse.title", IndexerBolt.forLogging("parse.title"));
    }
}
