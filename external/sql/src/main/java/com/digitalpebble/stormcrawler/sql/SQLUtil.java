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

package com.digitalpebble.stormcrawler.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import com.digitalpebble.stormcrawler.util.ConfUtils;

public class SQLUtil {

    private SQLUtil() {
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static Connection getConnection(Map stormConf) throws SQLException {
        // SQL connection details
        String url = ConfUtils.getString(stormConf,
                Constants.MYSQL_URL_PARAM_NAME,
                "jdbc:mysql://localhost:3306/crawl");
        String user = ConfUtils.getString(stormConf,
                Constants.MYSQL_USER_PARAM_NAME);
        String password = ConfUtils.getString(stormConf,
                Constants.MYSQL_PASSWORD_PARAM_NAME);
        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", password);
        props.setProperty("rewriteBatchedStatements", "true");
        props.setProperty("useBatchMultiSend", "true");
        return DriverManager.getConnection(url, props);
    }

}
