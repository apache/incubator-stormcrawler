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
package com.digitalpebble.stormcrawler.opensearch;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URL;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.CreateIndexResponse;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.indices.IndexTemplatesExistRequest;
import org.opensearch.client.indices.PutIndexTemplateRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;

public class IndexCreation {

    public static synchronized void checkOrCreateIndex(
            RestHighLevelClient client, String indexName, Logger log) throws IOException {
        final boolean indexExists =
                client.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);
        log.info("Index '{}' exists? {}", indexName, indexExists);
        if (!indexExists) {
            boolean created = IndexCreation.createIndex(client, indexName, indexName + ".mapping");
            log.info("Index '{}' created? {}", indexName, created);
        }
    }

    public static synchronized void checkOrCreateIndexTemplate(
            RestHighLevelClient client, String resourceName, Logger log) throws IOException {
        final String templateName = resourceName + "-template";
        final boolean templateExists =
                client.indices()
                        .existsTemplate(
                                new IndexTemplatesExistRequest(templateName),
                                RequestOptions.DEFAULT);
        log.info("Template '{}' exists? {}", templateName, templateExists);
        if (!templateExists) {
            boolean created =
                    IndexCreation.createTemplate(client, templateName, resourceName + ".mapping");
            log.info("templateExists '{}' created? {}", templateName, created);
        }
    }

    private static boolean createTemplate(
            RestHighLevelClient client, String templateName, String resourceName) {

        try {
            final PutIndexTemplateRequest createIndexRequest =
                    new PutIndexTemplateRequest(templateName);

            final URL mapping =
                    Thread.currentThread().getContextClassLoader().getResource(resourceName);

            final String jsonIndexConfiguration = Resources.toString(mapping, Charsets.UTF_8);

            createIndexRequest.source(jsonIndexConfiguration, XContentType.JSON);

            final AcknowledgedResponse createIndexResponse =
                    client.indices().putTemplate(createIndexRequest, RequestOptions.DEFAULT);
            return createIndexResponse.isAcknowledged();
        } catch (IOException e) {
            return false;
        }
    }

    private static boolean createIndex(
            RestHighLevelClient client, String indexName, String resourceName) {

        try {

            final CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);

            final URL mapping =
                    Thread.currentThread().getContextClassLoader().getResource(resourceName);

            final String jsonIndexConfiguration = Resources.toString(mapping, Charsets.UTF_8);

            createIndexRequest.source(jsonIndexConfiguration, XContentType.JSON);

            final CreateIndexResponse createIndexResponse =
                    client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            return createIndexResponse.isAcknowledged();
        } catch (IOException e) {
            return false;
        }
    }
}
