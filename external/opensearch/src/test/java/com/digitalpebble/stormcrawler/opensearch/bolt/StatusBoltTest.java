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
package com.digitalpebble.stormcrawler.opensearch.bolt;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.TestOutputCollector;
import com.digitalpebble.stormcrawler.TestUtil;
import com.digitalpebble.stormcrawler.opensearch.persistence.StatusUpdaterBolt;
import com.digitalpebble.stormcrawler.persistence.Status;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.http.HttpHost;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

public class StatusBoltTest {

    private StatusUpdaterBolt bolt;
    protected TestOutputCollector output;

    protected org.opensearch.client.RestHighLevelClient client;

    private static final Logger LOG = LoggerFactory.getLogger(StatusBoltTest.class);
    private static ExecutorService executorService;

    @Rule public Timeout globalTimeout = Timeout.seconds(60);

    @Rule
    public final GenericContainer container =
            new GenericContainer(DockerImageName.parse("opensearchproject/opensearch:latest"))
                    .withExposedPorts(9200)
                    .withEnv("plugins.security.disabled", "true")
                    .withEnv("discovery.type", "single-node")
                    .withEnv("OPENSEARCH_JAVA_OPTS", "-Xms512m -Xmx512m");

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

        bolt = new StatusUpdaterBolt();

        RestClientBuilder builder =
                RestClient.builder(
                        new HttpHost(container.getHost(), container.getMappedPort(9200)));

        client = new RestHighLevelClient(builder);

        // configure the status updater bolt

        Map<String, Object> conf = new HashMap<>();
        conf.put("opensearch.status.routing.fieldname", "metadata.key");

        conf.put(
                "opensearch.status.addresses",
                container.getHost() + ":" + container.getFirstMappedPort());

        conf.put("scheduler.class", "com.digitalpebble.stormcrawler.persistence.DefaultScheduler");

        conf.put("status.updater.cache.spec", "maximumSize=10000,expireAfterAccess=1h");

        conf.put("metadata.persist", "someKey");

        output = new TestOutputCollector();

        bolt.prepare(conf, TestUtil.getMockedTopologyContext(), new OutputCollector(output));
    }

    @After
    public void close() {
        LOG.info("Closing updater bolt and Opensearch container");
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
    public void checkListKeyFromOpensearch()
            throws IOException, ExecutionException, InterruptedException, TimeoutException {

        String url = "https://www.url.net/something";

        Metadata md = new Metadata();

        md.addValue("someKey", "someValue");

        store(url, Status.DISCOVERED, md).get(10, TimeUnit.SECONDS);

        assertEquals(1, output.getAckedTuples().size());

        // check output in Opensearch?

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
