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

public class Constants {

    public static final String MYSQL_URL_PARAM_NAME = "mysql.url";
    public static final String MYSQL_USER_PARAM_NAME = "mysql.user";
    public static final String MYSQL_PASSWORD_PARAM_NAME = "mysql.password";
    public static final String MYSQL_TABLE_PARAM_NAME = "mysql.table";
    public static final String MYSQL_BUFFERSIZE_PARAM_NAME = "mysql.buffer.size";
    public static final String MYSQL_MAX_DOCS_BUCKET_PARAM_NAME = "mysql.max.urls.per.bucket";
    public static final String MYSQL_MIN_QUERY_INTERVAL_PARAM_NAME = "mysql.min.query.interval";

    private Constants() {
    }
}
