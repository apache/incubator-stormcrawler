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
package org.apache.stormcrawler.solr.persistence;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.Timeout;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

public abstract class SolrContainerTest {
    @Rule public Timeout globalTimeout = Timeout.seconds(120);

    protected static ExecutorService executorService;

    private final DockerImageName image = DockerImageName.parse("solr:9.1");
    private static final String coresPath = new File("cores").getAbsolutePath();

    @Rule
    public GenericContainer<?> container =
            new GenericContainer<>(image)
                    .withExposedPorts(8983)
                    .withCopyFileToContainer(
                            MountableFile.forHostPath(coresPath), "/opt/solr/server/solr/cores")
                    .waitingFor(Wait.forHttp("/solr/admin/cores?action=STATUS").forStatusCode(200));

    @BeforeClass
    public static void beforeClass() {
        executorService = Executors.newFixedThreadPool(2);
    }

    @AfterClass
    public static void afterClass() {
        executorService.shutdown();
        executorService = null;
    }

    protected String getSolrBaseUrl() {
        return "http://" + container.getHost() + ":" + container.getMappedPort(8983) + "/solr";
    }

    protected Container.ExecResult createCore(String coreName)
            throws IOException, InterruptedException {
        return container.execInContainer(
                "/opt/solr/bin/solr",
                "create",
                "-c",
                coreName,
                "-d",
                "/opt/solr/server/solr/cores/" + coreName);
    }
}
