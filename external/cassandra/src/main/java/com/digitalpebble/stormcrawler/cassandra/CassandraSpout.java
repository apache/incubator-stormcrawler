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

package com.digitalpebble.stormcrawler.cassandra;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.digitalpebble.stormcrawler.util.StringTabScheme;

@SuppressWarnings("serial")
public class CassandraSpout extends BaseRichSpout {

    public static final Logger LOG = LoggerFactory
            .getLogger(CassandraSpout.class);

    private static final StringTabScheme SCHEME = new StringTabScheme();

    private SpoutOutputCollector _collector;

    private int bufferSize = 100;

    private Queue<List<Object>> buffer = new LinkedList<>();

    /**
     * Keeps track of the URLs in flight so that we don't add them more than
     * once when the table contains just a few URLs
     **/
    private Set<String> beingProcessed = new HashSet<>();

    private boolean active;

    private MultiCountMetric eventCounter;

    private int minWaitBetweenQueriesMSec = 5000;

    private long lastQueryTime = System.currentTimeMillis();

    private Cluster cluster;

    private Session session;

    private String tableName = "stormcrawler.webpage";

    private String lastHostName = "";

    private Map config;

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void open(Map conf, TopologyContext context,
            SpoutOutputCollector collector) {
        _collector = collector;
        config = conf;
        connect();
    }

    private void connect() {
        // TODO get connection details from config
        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(SCHEME.getOutputFields());
    }

    @Override
    public void nextTuple() {
        if (!active)
            return;

        if (!buffer.isEmpty()) {
            List<Object> fields = buffer.remove();
            String url = fields.get(0).toString();
            this._collector.emit(fields, url);
            beingProcessed.add(url);
            return;
        }

        // re-populate the buffer
        long now = System.currentTimeMillis();
        long allowed = lastQueryTime + minWaitBetweenQueriesMSec;
        if (now > allowed) {
            populateBuffer();
            lastQueryTime = now;
        }
    }

    private void populateBuffer() {
        // select entries from mysql
        String query = "SELECT url, metadata, hostname FROM " + tableName;
        query += " WHERE next_fetch_date <= '" + new Date().getTime() + "'";
        query += " AND token(hostname) > token('" + lastHostName + "')";
        query += " PER PARTITION LIMIT 2";
        query += " LIMIT " + this.bufferSize;
        query += " ALLOW FILTERING";

        // TODO async
        ResultSet rs = session.execute(query);

        Iterator<Row> row = rs.iterator();

        // no results? try resetting the hostname
        if (!row.hasNext()) {
            lastHostName = "";
        }

        while (row.hasNext()) {
            Row r = row.next();
            String url = r.getString("url");
            // already processed? skip
            if (beingProcessed.contains(url)) {
                continue;
            }
            String host = r.getString("hostname");
            LOG.debug("HOSTNAME {}", host);
            lastHostName = host;

            String metadata = r.getString("metadata");
            if (metadata == null) {
                metadata = "";
            } else if (!metadata.startsWith("\t")) {
                metadata = "\t" + metadata;
            }
            String URLMD = url + metadata;
            List<Object> v = SCHEME.deserialize(ByteBuffer.wrap(URLMD
                    .getBytes()));
            buffer.add(v);
        }
    }

    @Override
    public void activate() {
        active = true;
        connect();
    }

    @Override
    public void deactivate() {
        active = false;
        close();
    }

    @Override
    public void ack(Object msgId) {
        beingProcessed.remove(msgId);
    }

    @Override
    public void fail(Object msgId) {
        beingProcessed.remove(msgId);
    }

    @Override
    public void close() {
        if (cluster != null) {
            cluster.close();
            cluster = null;
        }
    }
}
