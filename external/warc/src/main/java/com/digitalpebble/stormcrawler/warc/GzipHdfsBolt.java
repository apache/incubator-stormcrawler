/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.digitalpebble.stormcrawler.warc;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.io.output.CountingOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.storm.hdfs.bolt.AbstractHdfsBolt;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hdfs.common.rotation.RotationAction;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Unlike the standard HdfsBolt this one writes to a gzipped stream **/
public class GzipHdfsBolt extends AbstractHdfsBolt {
    private static final Logger LOG = LoggerFactory
            .getLogger(GzipHdfsBolt.class);

    private transient OutputStream out = null;

    private RecordFormat format;

    private boolean rotateOnCompressedOffset = false;

    public static class CompressedOutputStream extends GZIPOutputStream {
        public CompressedOutputStream(OutputStream out) throws IOException {
            super(out);
        }

        public void end() {
            def.end();
        }
    }

    public GzipHdfsBolt withFsUrl(String fsUrl) {
        this.fsUrl = fsUrl;
        return this;
    }

    public GzipHdfsBolt withConfigKey(String configKey) {
        this.configKey = configKey;
        return this;
    }

    public GzipHdfsBolt withFileNameFormat(FileNameFormat fileNameFormat) {
        this.fileNameFormat = fileNameFormat;
        return this;
    }

    public GzipHdfsBolt withRecordFormat(RecordFormat format) {
        this.format = format;
        return this;
    }

    public GzipHdfsBolt withSyncPolicy(SyncPolicy syncPolicy) {
        this.syncPolicy = syncPolicy;
        return this;
    }

    public GzipHdfsBolt withRotationPolicy(FileRotationPolicy rotationPolicy) {
        this.rotationPolicy = rotationPolicy;
        return this;
    }

    public GzipHdfsBolt addRotationAction(RotationAction action) {
        this.rotationActions.add(action);
        return this;
    }

    public GzipHdfsBolt setRotationCompressedSizeOffset(
            boolean useCompressedOffset) {
        this.rotateOnCompressedOffset = useCompressedOffset;
        return this;
    }

    @Override
    public void doPrepare(Map conf, TopologyContext topologyContext,
            OutputCollector collector) throws IOException {
        LOG.info("Preparing HDFS Bolt...");
        this.fs = FileSystem.get(URI.create(this.fsUrl), hdfsConfig);
    }

    protected void writeRecord(byte[] bytes) throws IOException {
        CountingOutputStream countingStream = new CountingOutputStream(out);
        @SuppressWarnings("resource")
        CompressedOutputStream compressedStream = new CompressedOutputStream(
                countingStream);
        synchronized (this.writeLock) {
            compressedStream.write(bytes);
            compressedStream.finish();
            compressedStream.flush();
        }
        compressedStream.end();
        if (rotateOnCompressedOffset) {
            offset += countingStream.getByteCount();
        } else {
            offset += bytes.length;
        }
    }

    @Override
    public void writeTuple(Tuple tuple) throws IOException {
        writeRecord(this.format.format(tuple));
    }

    @Override
    protected void closeOutputFile() throws IOException {
        this.out.close();
    }

    @Override
    protected Path createOutputFile() throws IOException {
        Path path = new Path(this.fileNameFormat.getPath(),
                this.fileNameFormat.getName(this.rotation,
                        System.currentTimeMillis()));
        this.out = this.fs.create(path);
        return path;
    }

    @Override
    protected void syncTuples() throws IOException {
        this.out.flush();
    }

    @Override
    public void cleanup() {
        LOG.info("Cleanup called on bolt");
        try {
            this.out.flush();
            this.out.close();
        } catch (IOException e) {
            LOG.error("Exception while calling cleanup");
        }
    }
}
