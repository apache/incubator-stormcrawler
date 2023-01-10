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
package com.digitalpebble.stormcrawler.sql;

import static com.digitalpebble.stormcrawler.Constants.StatusStreamName;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.indexing.AbstractIndexerBolt;
import com.digitalpebble.stormcrawler.persistence.Status;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Stores URL and selected metadata into a SQL table * */
public class IndexerBolt extends AbstractIndexerBolt {

    private static final Logger LOG = LoggerFactory.getLogger(IndexerBolt.class);

    public static final String SQL_INDEX_TABLE_PARAM_NAME = "sql.index.table";

    private OutputCollector _collector;

    private MultiCountMetric eventCounter;

    private Connection connection;

    private String tableName;

    private Map conf;

    @Override
    public void prepare(
            Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
        super.prepare(conf, context, collector);
        _collector = collector;

        this.eventCounter = context.registerMetric("SQLIndexer", new MultiCountMetric(), 10);

        this.tableName = ConfUtils.getString(conf, SQL_INDEX_TABLE_PARAM_NAME);

        this.conf = conf;
    }

    @Override
    public void execute(Tuple tuple) {
        String url = tuple.getStringByField("url");

        // Distinguish the value used for indexing
        // from the one used for the status
        String normalisedurl = valueForURL(tuple);

        Metadata metadata = (Metadata) tuple.getValueByField("metadata");
        String text = tuple.getStringByField("text");

        boolean keep = filterDocument(metadata);
        if (!keep) {
            eventCounter.scope("Filtered").incrBy(1);
            // treat it as successfully processed even if
            // we do not index it
            _collector.emit(StatusStreamName, tuple, new Values(url, metadata, Status.FETCHED));
            _collector.ack(tuple);
            return;
        }

        try {

            // which metadata to display?
            Map<String, String[]> keyVals = filterMetadata(metadata);

            StringBuilder query =
                    new StringBuilder(" insert into ")
                            .append(tableName)
                            .append(" (")
                            .append(fieldNameForURL());

            Object[] keys = keyVals.keySet().toArray();

            for (int i = 0; i < keys.length; i++) {
                query.append(", ").append((String) keys[i]);
            }

            query.append(") values(?");

            for (int i = 0; i < keys.length; i++) {
                query.append(", ?");
            }

            query.append(")");

            query.append(" ON DUPLICATE KEY UPDATE ");
            for (int i = 0; i < keys.length; i++) {
                String key = (String) keys[i];
                if (i > 0) {
                    query.append(", ");
                }
                query.append(key).append("=VALUES(").append(key).append(")");
            }

            if (connection == null) {
                try {
                    connection = SQLUtil.getConnection(conf);
                } catch (SQLException ex) {
                    LOG.error(ex.getMessage(), ex);
                    throw new RuntimeException(ex);
                }
            }

            LOG.debug("PreparedStatement => {}", query);

            // create the mysql insert preparedstatement
            PreparedStatement preparedStmt = connection.prepareStatement(query.toString());

            // TODO store the text of the document?
            if (StringUtils.isNotBlank(fieldNameForText())) {
                // builder.field(fieldNameForText(), trimText(text));
            }

            // send URL as field?
            if (fieldNameForURL() != null) {
                preparedStmt.setString(1, normalisedurl);
            }

            for (int i = 0; i < keys.length; i++) {
                insert(preparedStmt, i + 2, (String) keys[i], keyVals);
            }

            preparedStmt.executeUpdate();

            eventCounter.scope("Indexed").incrBy(1);

            _collector.emit(StatusStreamName, tuple, new Values(url, metadata, Status.FETCHED));
            _collector.ack(tuple);

        } catch (Exception e) {
            // do not send to status stream so that it gets replayed
            LOG.error("Error inserting into SQL", e);
            _collector.fail(tuple);
            if (connection != null) {
                // reset the connection
                try {
                    connection.close();
                } catch (SQLException e1) {
                }
                connection = null;
            }
        }
    }

    private void insert(
            PreparedStatement preparedStmt,
            int position,
            String label,
            Map<String, String[]> keyVals)
            throws SQLException {
        String[] values = keyVals.get(label);
        String value = "";
        if (values == null || values.length == 0) {
            LOG.info("No values found for label {}", label);
        } else if (values.length > 1) {
            LOG.info("More than one value found for label {}", label);
            value = values[0];
        } else {
            value = values[0];
        }
        preparedStmt.setString(position, value);
    }
}
