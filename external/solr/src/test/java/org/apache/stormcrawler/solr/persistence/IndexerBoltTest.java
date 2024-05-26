package org.apache.stormcrawler.solr.persistence;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.TestOutputCollector;
import org.apache.stormcrawler.TestUtil;
import org.apache.stormcrawler.indexing.AbstractIndexerBolt;
import org.apache.stormcrawler.solr.bolt.IndexerBolt;
import org.junit.*;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

public class IndexerBoltTest {
    @Rule public Timeout globalTimeout = Timeout.seconds(120);

    private IndexerBolt bolt;
    protected TestOutputCollector output;

    private static final Logger LOG = LoggerFactory.getLogger(IndexerBoltTest.class);
    private static ExecutorService executorService;

    private final DockerImageName image = DockerImageName.parse("solr:9.1");

    @Rule
    public GenericContainer<?> container =
            new GenericContainer<>(image)
                    .withExposedPorts(8983)
                    .withCopyFileToContainer(
                            MountableFile.forClasspathResource("/cores/"),
                            "/opt/solr/server/solr/cores")
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

    @Before
    public void setupIndexerBolt() throws IOException, InterruptedException {
        container.start();
        container.execInContainer(
                "/opt/solr/bin/solr",
                "create",
                "-c",
                "docs",
                "-d",
                "/opt/solr/server/solr/cores/docs");

        bolt = new IndexerBolt();
        output = new TestOutputCollector();

        Map<String, Object> conf = new HashMap<>();
        conf.put(AbstractIndexerBolt.urlFieldParamName, "url");
        conf.put(AbstractIndexerBolt.textFieldParamName, "content");
        conf.put(AbstractIndexerBolt.canonicalMetadataParamName, "canonical");

        final String SOLRURL =
                "http://"
                        + container.getHost()
                        + ":"
                        + container.getMappedPort(8983)
                        + "/solr/docs";

        conf.put("solr.indexer.url", SOLRURL);

        bolt.prepare(conf, TestUtil.getMockedTopologyContext(), new OutputCollector(output));
    }

    @After
    public void close() {
        LOG.info("Closing indexer bolt and Solr container");
        bolt.cleanup();
        container.close();
        output = null;
    }

    private void index(String url, String text, Metadata metadata) {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getStringByField("text")).thenReturn(text);
        when(tuple.getStringByField("url")).thenReturn(url);
        when(tuple.getValueByField("metadata")).thenReturn(metadata);
        bolt.execute(tuple);
    }

    @Test
    public void basicTest() {
        Metadata m = new Metadata();
        String url =
                "https://www.obozrevatel.com/ukr/dnipro/city/u-dnipri-ta-oblasti-ogolosili-shtormove-poperedzhennya.htm";
        m.addValue("canonical", url);
        index(url, "", m);
        Assert.assertEquals(1, output.getAckedTuples().size());
    }
}
