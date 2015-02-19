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

package com.digitalpebble.storm.crawler.persistence;

import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import com.digitalpebble.storm.crawler.Constants;
import com.digitalpebble.storm.crawler.Metadata;
import com.digitalpebble.storm.crawler.util.ConfUtils;

/**
 * Schedules a nextFetchDate based on the configuration
 **/
public class DefaultScheduler implements Scheduler {

    // fetch intervals in minutes
    private int defaultfetchInterval;
    private int fetchErrorFetchInterval;
    private int errorFetchInterval;

    /*
     * (non-Javadoc)
     * 
     * @see com.shopstyle.discovery.crawler.Scheduler#init(java.util.Map)
     */
    @SuppressWarnings("rawtypes")
    @Override
    public void init(Map stormConf) {
        defaultfetchInterval = ConfUtils.getInt(stormConf,
                Constants.defaultFetchIntervalParamName, 1440);
        fetchErrorFetchInterval = ConfUtils.getInt(stormConf,
                Constants.fetchErrorFetchIntervalParamName, 120);
        errorFetchInterval = ConfUtils.getInt(stormConf,
                Constants.errorFetchIntervalParamName, 44640);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.shopstyle.discovery.crawler.Scheduler#schedule(com.digitalpebble.
     * storm.crawler.persistence .Status,
     * com.digitalpebble.storm.crawler.Metadata)
     */
    @Override
    public Date schedule(Status status, Metadata metadata) {

        Calendar cal = Calendar.getInstance();

        switch (status) {
        case FETCHED:
            cal.add(Calendar.MINUTE, defaultfetchInterval);
            break;
        case FETCH_ERROR:
            cal.add(Calendar.MINUTE, fetchErrorFetchInterval);
            break;
        case ERROR:
            cal.add(Calendar.MINUTE, errorFetchInterval);
            break;
        case REDIRECTION:
            cal.add(Calendar.MINUTE, defaultfetchInterval);
            break;
        default:
            // leave it to now e.g. DISCOVERED
        }

        return cal.getTime();
    }
}
