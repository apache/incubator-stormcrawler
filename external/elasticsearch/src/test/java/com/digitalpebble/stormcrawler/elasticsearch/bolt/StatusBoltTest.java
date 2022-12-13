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
package com.digitalpebble.stormcrawler.elasticsearch.bolt;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.TestOutputCollector;
import com.digitalpebble.stormcrawler.TestUtil;
import com.digitalpebble.stormcrawler.elasticsearch.persistence.StatusUpdaterBolt;
import com.digitalpebble.stormcrawler.persistence.Status;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.xcontent.XContentType;
import org.junit.*;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

public class StatusBoltTest {

    private ElasticsearchContainer container;
    private StatusUpdaterBolt bolt;
    protected TestOutputCollector output;

    protected RestHighLevelClient client;

    private static final Logger LOG = LoggerFactory.getLogger(StatusBoltTest.class);
    private static ExecutorService executorService;

    @Rule public Timeout globalTimeout = Timeout.seconds(120);

    @BeforeClass
    public static void beforeClass() {
        executorService = Executors.newFixedThreadPool(2);
    }

    @AfterClass
    public static void afterClass() {
        executorService.shutdown();
        executorService = null;
    }

    @Before
    public void setupStatusBolt() throws IOException {

        String version = System.getProperty("elasticsearch-version");
        if (version == null) version = "7.17.7";
        LOG.info("Starting docker instance of Elasticsearch {}...", version);

        container =
                new ElasticsearchContainer(
                                "docker.elastic.co/elasticsearch/elasticsearch:" + version)
                        .withPassword("s3cret");

        container.start();

        bolt = new StatusUpdaterBolt();

        // configure the status index

        RestClientBuilder builder =
                RestClient.builder(
                        new HttpHost(container.getHost(), container.getMappedPort(9200)));

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(
                AuthScope.ANY, new UsernamePasswordCredentials("elastic", "s3cret"));

        builder.setHttpClientConfigCallback(
                clientBuilder -> {
                    clientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    return clientBuilder;
                });

        client = new RestHighLevelClient(builder);

        // TODO
        // https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.13/java-rest-high-put-mapping.html

        CreateIndexRequest request = new CreateIndexRequest("status");

        URI uriToFile;
        try {
            uriToFile =
                    Objects.requireNonNull(
                                    getClass().getClassLoader().getResource("status.mapping"))
                            .toURI();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        String mappingSource = Files.readString(Path.of(uriToFile), Charset.defaultCharset());

        request.source(mappingSource, XContentType.JSON);

        client.indices().create(request, RequestOptions.DEFAULT);

        // configure the status updater bolt

        Map<String, Object> conf = new HashMap<>();
        conf.put("es.status.routing.fieldname", "metadata.key");

        conf.put("es.status.addresses", container.getHttpHostAddress());

        conf.put("scheduler.class", "com.digitalpebble.stormcrawler.persistence.DefaultScheduler");

        conf.put("status.updater.cache.spec", "maximumSize=10000,expireAfterAccess=1h");

        conf.put("metadata.persist", "someKey");

        conf.put("es.status.compatibility.mode", false);

        conf.put("es.status.user", "elastic");
        conf.put("es.status.password", "s3cret");

        output = new TestOutputCollector();

        bolt.prepare(conf, TestUtil.getMockedTopologyContext(), new OutputCollector(output));
    }

    @After
    public void close() {
        LOG.info("Closing updater bolt and ES container");
        bolt.cleanup();
        container.close();
        output = null;
        try {
            client.close();
        } catch (IOException e) {
        }
    }

    private Future<Integer> store(String url, Status status, Metadata metadata) {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getValueByField("status")).thenReturn(status);
        when(tuple.getStringByField("url")).thenReturn(url);
        when(tuple.getValueByField("metadata")).thenReturn(metadata);
        bolt.execute(tuple);

        return executorService.submit(
                () -> {
                    var outputSize = output.getAckedTuples().size();
                    while (outputSize == 0) {
                        Thread.sleep(100);
                        outputSize = output.getAckedTuples().size();
                    }
                    return outputSize;
                });
    }

    @Test
    // see https://github.com/DigitalPebble/storm-crawler/issues/885
    public void checkListKeyFromES()
            throws IOException, ExecutionException, InterruptedException, TimeoutException {

        String url = "https://www.url.net/something";

        Metadata md = new Metadata();

        md.addValue("someKey", "someValue");

        store(url, Status.DISCOVERED, md).get(10, TimeUnit.SECONDS);

        assertEquals(1, output.getAckedTuples().size());

        // check output in ES?

        String id = org.apache.commons.codec.digest.DigestUtils.sha256Hex(url);

        GetResponse result = client.get(new GetRequest("status", id), RequestOptions.DEFAULT);

        Map<String, Object> sourceAsMap = result.getSourceAsMap();

        final String pfield = "metadata.someKey";
        sourceAsMap = (Map<String, Object>) sourceAsMap.get("metadata");

        final var pfieldNew = pfield.substring(9);
        Object key = sourceAsMap.get(pfieldNew);

        assertTrue(key instanceof java.util.ArrayList);
    }
}
