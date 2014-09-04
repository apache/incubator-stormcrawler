/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
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

package com.digitalpebble.storm.crawler.spout;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.LinkedList;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import backtype.storm.spout.Scheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;

/**
 * Reads the lines from a file and use them as a spout. Load the entire content
 * into memory
 ***/

public class FileSpout extends BaseRichSpout {

    private SpoutOutputCollector _collector;
    private String _inputFile;
    private Scheme _scheme;

    private LinkedList<byte[]> toPut = new LinkedList<byte[]>();

    public FileSpout(String inputFile, Scheme scheme) {
        if (StringUtils.isBlank(inputFile)) {
            throw new IllegalArgumentException(
                    "Must configure at least one inputFile");
        }
        _scheme = scheme;
        _inputFile = inputFile;

    }

    @Override
    public void open(Map conf, TopologyContext context,
            SpoutOutputCollector collector) {
        _collector = collector;

        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(new File(_inputFile)));
            String line = null;
            while ((line = reader.readLine()) != null) {
                if (StringUtils.isBlank(line))
                    continue;
                toPut.add(line.getBytes("UTF-8"));
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                reader.close();
            } catch (Exception e) {
            }
        }
    }

    @Override
    public void nextTuple() {
        if (toPut.isEmpty())
            return;
        byte[] head = toPut.removeFirst();
        this._collector.emit(this._scheme.deserialize(head));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(_scheme.getOutputFields());
    }

    public void close() {
    }

}
