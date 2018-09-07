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
import java.net.URI;
import java.util.Map;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.storm.hdfs.bolt.AbstractHdfsBolt;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hdfs.common.AbstractHDFSWriter;
import org.apache.storm.hdfs.common.HDFSWriter;
import org.apache.storm.hdfs.common.rotation.RotationAction;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unlike the standard HdfsBolt this one writes to a gzipped stream with
 * per-record compression.
 **/
@SuppressWarnings("serial")
public class GzipHdfsBolt extends AbstractHdfsBolt {

    private static final Logger LOG = LoggerFactory
            .getLogger(GzipHdfsBolt.class);

    protected transient FSDataOutputStream out = null;

    protected RecordFormat format;

    public static class GzippedRecordFormat implements RecordFormat {

        private RecordFormat baseFormat;

        public GzippedRecordFormat(RecordFormat format) {
            baseFormat = format;
        }

        @Override
        public byte[] format(Tuple tuple) {
            byte[] bytes = baseFormat.format(tuple);
            return Utils.gzip(bytes);
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
        this.format = new GzippedRecordFormat(format);
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

    @Override
    public void doPrepare(Map conf, TopologyContext topologyContext,
            OutputCollector collector) throws IOException {
        LOG.info("Preparing HDFS Bolt...");
        this.fs = FileSystem.get(URI.create(this.fsUrl), hdfsConfig);
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

    @Override
    protected String getWriterKey(Tuple tuple) {
        return "CONSTANT";
    }

    @Override
    protected AbstractHDFSWriter makeNewWriter(Path path, Tuple tuple)
            throws IOException {
        out = this.fs.create(path);
        return new HDFSWriter(rotationPolicy, path, out, format);
    }
}
