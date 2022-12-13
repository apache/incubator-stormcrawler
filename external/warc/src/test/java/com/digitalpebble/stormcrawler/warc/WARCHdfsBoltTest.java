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
package com.digitalpebble.stormcrawler.warc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.TestOutputCollector;
import com.digitalpebble.stormcrawler.TestUtil;
import com.digitalpebble.stormcrawler.protocol.ProtocolResponse;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.netpreserve.jwarc.MessageVersion;
import org.netpreserve.jwarc.WarcReader;
import org.netpreserve.jwarc.WarcRecord;
import org.netpreserve.jwarc.WarcRequest;
import org.netpreserve.jwarc.WarcResponse;

public class WARCHdfsBoltTest {

    private String protocolMDprefix = "protocol.";
    private Path warcDir = Path.of("target", "warc");

    private TestOutputCollector output;
    private HdfsBolt bolt;
    private Map<String, Object> conf;

    @Before
    public void setup() throws IOException {
        output = new TestOutputCollector();

        // create directory for WARC files and cleanup
        Files.createDirectories(warcDir);
        Files.walk(warcDir)
                .forEach(
                        t -> {
                            try {
                                Files.delete(t);
                            } catch (IOException e) {
                            }
                        });

        bolt = makeBolt();

        // configure RawLocalFileSystem so that WARC files are immediately flushed
        bolt.withConfigKey("warc");
        Map<String, Object> hdfsConf = new HashMap<>();
        hdfsConf.put("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
        conf = new HashMap<String, Object>();
        conf.put("warc", hdfsConf);

        conf.put(ProtocolResponse.PROTOCOL_MD_PREFIX_PARAM, protocolMDprefix);

        bolt.prepare(conf, TestUtil.getMockedTopologyContext(), new OutputCollector(output));
    }

    @After
    public void cleanup() {
        bolt.cleanup();
        output = null;
    }

    @Test
    public void test() throws IOException {
        // write page into WARC file
        bolt.execute(getPage());

        // ensure the WARC file is written
        bolt.cleanup();

        // read WARC file
        List<WarcRecord> records = readWARCs(warcDir).collect(Collectors.toList());

        // expected 3 records (warcinfo, request, response)
        assertEquals(3, records.size());
        assertEquals("warcinfo", records.get(0).type());
        assertEquals("request", records.get(1).type());
        assertEquals("response", records.get(2).type());
        WarcResponse response = (WarcResponse) records.get(2);
        assertEquals(MessageVersion.HTTP_1_1, response.http().version());
        assertTrue(
                "WARC response record is expected to include WARC header \"WARC-Protocol\"",
                response.headers().first("WARC-Protocol").isPresent());
        assertTrue(
                "WARC response record is expected to include WARC header \"WARC-IP-Address\"",
                response.headers().first("WARC-IP-Address").isPresent());
    }

    @Test
    public void testHttp2() throws IOException {
        bolt.execute(getPage("HTTP/2"));
        bolt.cleanup();
        List<WarcRecord> records = readWARCs(warcDir).collect(Collectors.toList());

        // expected 3 records (warcinfo, request, response)
        assertEquals(3, records.size());
        assertEquals("warcinfo", records.get(0).type());
        assertEquals("request", records.get(1).type());
        WarcRequest request = (WarcRequest) records.get(1);
        assertEquals(MessageVersion.HTTP_1_1, request.http().version());
        assertEquals("response", records.get(2).type());
        WarcResponse response = (WarcResponse) records.get(2);
        assertEquals(MessageVersion.HTTP_1_1, response.http().version());
        assertTrue(
                "WARC response record is expected to include WARC header \"WARC-Protocol\"",
                response.headers().first("WARC-Protocol").isPresent());
        assertTrue(
                "WARC response record is expected to include WARC header \"WARC-IP-Address\"",
                response.headers().first("WARC-IP-Address").isPresent());
    }

    private static Stream<WarcRecord> readWARCs(Path warcDir) {
        try {
            return Files.walk(warcDir)
                    .filter(Files::isRegularFile)
                    .flatMap(WARCHdfsBoltTest::readWARC);
        } catch (IOException e) {
            return Stream.empty();
        }
    }

    private static Stream<WarcRecord> readWARC(Path warcFile) {
        try (WarcReader warcReader = new WarcReader(FileChannel.open(warcFile))) {
            List<WarcRecord> records =
                    warcReader
                            .records()
                            .map(WARCHdfsBoltTest::readWARCrecord)
                            .collect(Collectors.toList());
            return records.stream();
        } catch (IOException e) {
            return Stream.empty();
        }
    }

    private static WarcRecord readWARCrecord(WarcRecord record) {
        try {
            // need to read the HTTP header, so that it gets parsed
            if (record instanceof WarcResponse) ((WarcResponse) record).http();
            if (record instanceof WarcRequest) ((WarcRequest) record).http();
        } catch (IOException e) {
        }
        return record;
    }

    private HdfsBolt makeBolt() {
        WARCHdfsBolt bolt = new WARCHdfsBolt();
        bolt.withFileNameFormat(
                new WARCFileNameFormat().withPath(warcDir.toAbsolutePath().toString()));
        bolt.withRecordFormat(new WARCRecordFormat(protocolMDprefix));
        bolt.addRecordFormat(new WARCRequestRecordFormat(protocolMDprefix), 0);
        bolt.withRequestRecords();
        return bolt;
    }

    private Tuple getPage() {
        return getPage("HTTP/1.1");
    }

    private Tuple getPage(String httpVersionString) {
        String txt = "abcdef";
        byte[] content = txt.getBytes(StandardCharsets.UTF_8);
        Metadata metadata = new Metadata();
        metadata.addValue(
                protocolMDprefix + ProtocolResponse.REQUEST_HEADERS_KEY, //
                "GET / "
                        + httpVersionString
                        + "\r\n" //
                        + "User-Agent: myBot/1.0 (https://example.org/bot/; bot@example.org)\r\n" //
                        + "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8\r\n" //
                        + "Accept-Language: en-us,en-gb,en;q=0.7,*;q=0.3\r\n" //
                        + "Accept-Encoding: br,gzip\r\n" //
                        + "Host: example.org\r\n" //
                        + "Connection: Keep-Alive\r\n\r\n");
        metadata.addValue(
                protocolMDprefix + ProtocolResponse.RESPONSE_HEADERS_KEY, //
                httpVersionString
                        + " 200 OK\r\n" //
                        + "Content-Type: text/html\r\n" //
                        + "Content-Encoding: gzip\r\n" //
                        + "Content-Length: 26\r\n" //
                        + "Connection: close\r\n\r\n");
        metadata.addValue(
                protocolMDprefix + ProtocolResponse.PROTOCOL_VERSIONS_KEY,
                httpVersionString + ",TLS_1_3,TLS_AES_256_GCM_SHA384");
        metadata.addValue(protocolMDprefix + ProtocolResponse.RESPONSE_IP_KEY, "123.123.123.123");
        Tuple tuple = mock(Tuple.class);
        when(tuple.getBinaryByField("content")).thenReturn(content);
        when(tuple.getStringByField("url")).thenReturn("https://www.example.org/");
        when(tuple.getValueByField("metadata")).thenReturn(metadata);
        return tuple;
    }
}
