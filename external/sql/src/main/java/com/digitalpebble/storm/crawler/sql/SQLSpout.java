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

package com.digitalpebble.storm.crawler.sql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.spout.Scheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;

import com.digitalpebble.storm.crawler.util.ConfUtils;
import com.digitalpebble.storm.crawler.util.StringTabScheme;

@SuppressWarnings("serial")
public class SQLSpout extends BaseRichSpout {

    public static final Logger LOG = LoggerFactory.getLogger(SQLSpout.class);

    private static final Scheme SCHEME = new StringTabScheme();

    private SpoutOutputCollector _collector;

    private String tableName;

    private Connection connection;

    private int bufferSize = 100;

    private Queue<List<Object>> buffer = new LinkedList<List<Object>>();

    /**
     * Keeps track of the URLs in flight so that we don't add them more than
     * once when the table contains just a few URLs
     **/
    private Set<String> beingProcessed = new HashSet<String>();

    private boolean active;

    private MultiCountMetric eventCounter;

    private int minWaitBetweenQueriesMSec = 5000;

    private long lastQueryTime = System.currentTimeMillis();

    /**
     * if more than one instance of the spout exist, each one is in charge of a
     * separate bucket value. This is used to ensure a good diversity of URLs.
     **/
    private int bucketNum = -1;

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void open(Map conf, TopologyContext context,
            SpoutOutputCollector collector) {
        _collector = collector;

        this.eventCounter = context.registerMetric("SQLSpout",
                new MultiCountMetric(), 10);

        bufferSize = ConfUtils.getInt(conf,
                Constants.MYSQL_BUFFERSIZE_PARAM_NAME, 100);

        minWaitBetweenQueriesMSec = ConfUtils.getInt(conf,
                Constants.MYSQL_MIN_QUERY_INTERVAL_PARAM_NAME, 5000);

        tableName = ConfUtils.getString(conf, Constants.MYSQL_TABLE_PARAM_NAME);

        try {
            connection = SQLUtil.getConnection(conf);
        } catch (SQLException ex) {
            LOG.error(ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }

        // determine bucket this spout instance will be in charge of
        int totalTasks = context
                .getComponentTasks(context.getThisComponentId()).size();
        if (totalTasks > 1) {
            bucketNum = context.getThisTaskIndex();
        }

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
        String query = "SELECT * FROM " + tableName;
        query += " WHERE nextfetchdate <= '"
                + new Timestamp(new Date().getTime()) + "'";

        // constraint on bucket num
        if (bucketNum >= 0) {
            query += " AND bucket = '" + bucketNum + "'";
        }

        query += " LIMIT " + this.bufferSize;

        // create the java statement
        Statement st = null;
        ResultSet rs = null;
        try {
            st = this.connection.createStatement();

            // execute the query, and get a java resultset
            rs = st.executeQuery(query);

            eventCounter.scope("SQL queries").incrBy(1);

            // iterate through the java resultset
            while (rs.next()) {
                String url = rs.getString("url");
                // already processed? skip
                if (beingProcessed.contains(url)) {
                    continue;
                }
                String metadata = rs.getString("metadata");
                if (metadata == null) {
                    metadata = "";
                } else if (!metadata.startsWith("\t")) {
                    metadata = "\t" + metadata;
                }
                String URLMD = url + metadata;
                List<Object> v = SCHEME.deserialize(URLMD.getBytes());
                buffer.add(v);
            }
        } catch (SQLException e) {
            LOG.error("Exception while querying table", e);
        } finally {
            try {
                if(rs != null)
                    rs.close();
            } catch (SQLException e) {
                LOG.error("Exception closing resultset", e);
            }
            try {
                if (st != null)
                    st.close();
            } catch (SQLException e) {
                LOG.error("Exception closing statement", e);
            }
        }
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

    @Override
    public void ack(Object msgId) {
        super.ack(msgId);
        beingProcessed.remove(msgId);
    }

    @Override
    public void fail(Object msgId) {
        super.fail(msgId);
        beingProcessed.remove(msgId);
    }

    @Override
    public void close() {
        super.close();
        try {
            connection.close();
        } catch (SQLException e) {
            LOG.error("Exception caught while closing SQL connection", e);
        }
    }
}
