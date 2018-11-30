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

package com.digitalpebble.stormcrawler.spout;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.spout.Scheme;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.persistence.Status;
import com.digitalpebble.stormcrawler.util.StringTabScheme;

/**
 * Reads the lines from a UTF-8 file and use them as a spout. Load the entire
 * content into memory. Uses StringTabScheme to parse the lines into URLs and
 * Metadata, generates tuples on the default stream.
 */
@SuppressWarnings("serial")
public class FileSpout extends BaseRichSpout {

    public static final int BATCH_SIZE = 10000;
    public static final Logger LOG = LoggerFactory.getLogger(FileSpout.class);

    private SpoutOutputCollector _collector;

    private Queue<String> _inputFiles;
    private BufferedReader currentBuffer;

    private Scheme _scheme = new StringTabScheme();

    private LinkedList<byte[]> buffer = new LinkedList<>();
    private boolean active;

    public FileSpout(String dir, String filter) {
        this(dir, filter, false);
    }

    public FileSpout(String... files) {
        this(false, files);
    }

    /**
     * @param withDiscoveredStatus
     *            whether the tuples generated should contain a Status field
     *            with DISCOVERED as value. Is used with the default
     *            StringTabScheme only.
     * @since 1.13
     **/
    public FileSpout(String dir, String filter, boolean withDiscoveredStatus) {
        Path pdir = Paths.get(dir);
        _inputFiles = new LinkedList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(pdir,
                filter)) {
            for (Path entry : stream) {
                String inputFile = entry.toAbsolutePath().toString();
                _inputFiles.add(inputFile);
                LOG.info("Input : {}", inputFile);
            }
        } catch (IOException ioe) {
            LOG.error("IOException: %s%n", ioe);
        }
        if (withDiscoveredStatus) {
            _scheme = new StringTabScheme(Status.DISCOVERED);
        }
    }

    /**
     * @param withDiscoveredStatus
     *            whether the tuples generated should contain a Status field
     *            with DISCOVERED as value. Is used with the default
     *            StringTabScheme only.
     * @since 1.13
     **/
    public FileSpout(boolean withDiscoveredStatus, String... files) {
        if (files.length == 0) {
            throw new IllegalArgumentException(
                    "Must configure at least one inputFile");
        }
        _inputFiles = new LinkedList<>();
        for (String f : files) {
            _inputFiles.add(f);
        }
        if (withDiscoveredStatus) {
            _scheme = new StringTabScheme(Status.DISCOVERED);
        }
    }

    /**
     * Specify a Scheme for parsing the lines into URLs and Metadata.
     * StringTabScheme is used by default. The Scheme must generate a String for
     * the URL and a Metadata object. If a Status field is needed, the scheme
     * must generate it.
     * 
     * @since 1.13
     **/
    public void setScheme(Scheme scheme) {
        _scheme = scheme;
    }

    private void populateBuffer() throws IOException {
        if (currentBuffer == null) {
            String file = _inputFiles.poll();
            if (file == null)
                return;
            Path inputPath = Paths.get(file);
            currentBuffer = new BufferedReader(new InputStreamReader(
                    new FileInputStream(inputPath.toFile()),
                    StandardCharsets.UTF_8));
        }

        // no more files to read from
        if (currentBuffer == null)
            return;

        String line = null;
        int linesRead = 0;
        while (linesRead < BATCH_SIZE
                && (line = currentBuffer.readLine()) != null) {
            if (StringUtils.isBlank(line))
                continue;
            if (line.startsWith("#"))
                continue;
            buffer.add(line.trim().getBytes(StandardCharsets.UTF_8));
            linesRead++;
        }

        // finished the file?
        if (line == null) {
            currentBuffer.close();
            currentBuffer = null;
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map conf, TopologyContext context,
            SpoutOutputCollector collector) {
        _collector = collector;

        try {
            populateBuffer();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void nextTuple() {
        if (!active)
            return;

        if (buffer.isEmpty()) {
            try {
                populateBuffer();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        // still empty?
        if (buffer.isEmpty())
            return;

        byte[] head = buffer.removeFirst();
        List<Object> fields = this._scheme.deserialize(ByteBuffer.wrap(head));
        this._collector.emit(fields, fields.get(0).toString());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(_scheme.getOutputFields());
    }

    @Override
    public void close() {
    }

    @Override
    public void activate() {
        super.activate();
        active = true;
    }

    @Override
    public void deactivate() {
        super.deactivate();
        active = false;
    }
}
