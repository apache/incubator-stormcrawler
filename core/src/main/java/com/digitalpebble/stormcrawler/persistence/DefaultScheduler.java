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

package com.digitalpebble.stormcrawler.persistence;

import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.digitalpebble.stormcrawler.Constants;
import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.util.ConfUtils;

/**
 * Schedules a nextFetchDate based on the configuration
 **/
public class DefaultScheduler extends Scheduler {

    /** Date far in the future used for never-refetch items. */
    public static final Date NEVER = new Calendar.Builder()
            .setCalendarType("iso8601").setDate(2099, Calendar.DECEMBER, 31)
            .build().getTime();

    // fetch intervals in minutes
    private int defaultfetchInterval;
    private int fetchErrorFetchInterval;
    private int errorFetchInterval;

    private CustomInterval[] customIntervals;

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.digitalpebble.stormcrawler.persistence.Scheduler#init(java.util.Map)
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

        // loads any custom key values
        // must be of form fetchInterval(.STATUS)?.keyname=value
        // e.g. fetchInterval.isFeed=true
        // e.g. fetchInterval.FETCH_ERROR.isFeed=true
        LinkedList<CustomInterval> intervals = new LinkedList<>();
        Pattern pattern = Pattern.compile("^fetchInterval(\\..+)?\\.(.+)=(.+)");
        Iterator<String> keyIter = stormConf.keySet().iterator();
        while (keyIter.hasNext()) {
            String key = keyIter.next();
            Matcher m = pattern.matcher(key);
            if (!m.matches()) {
                continue;
            }
            Status status = null;
            // was a status specified?
            if (m.group(1) != null) {
                status = Status.valueOf(m.group(1).substring(1));
            }
            String mdname = m.group(2);
            String mdvalue = m.group(3);
            int customInterval = ConfUtils.getInt(stormConf, key, -1);
            if (customInterval != -1) {
                CustomInterval interval = new CustomInterval(mdname, mdvalue,
                        status, customInterval);
                intervals.add(interval);
            }
        }
        customIntervals = intervals
                .toArray(new CustomInterval[intervals.size()]);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.digitalpebble.stormcrawler.persistence.Scheduler#schedule(com.
     * digitalpebble. stormcrawler.persistence .Status,
     * com.digitalpebble.stormcrawler.Metadata)
     */
    @Override
    public Date schedule(Status status, Metadata metadata) {

        int minutesIncrement = 0;

        Optional<Integer> customInterval = checkCustomInterval(metadata, status);

        if (customInterval.isPresent()) {
            minutesIncrement = customInterval.get();
        } else {
            switch (status) {
            case FETCHED:
                minutesIncrement = defaultfetchInterval;
                break;
            case FETCH_ERROR:
                minutesIncrement = fetchErrorFetchInterval;
                break;
            case ERROR:
                minutesIncrement = errorFetchInterval;
                break;
            case REDIRECTION:
                minutesIncrement = defaultfetchInterval;
                break;
            default:
                // leave it to now e.g. DISCOVERED
            }
        }

        // a value of -1 means never fetch
        // we use a conventional value
        if (minutesIncrement == -1) {
            return NEVER;
        }

        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.MINUTE, minutesIncrement);

        return cal.getTime();
    }

    /**
     * Returns the first matching custom interval
     **/
    protected final Optional<Integer> checkCustomInterval(Metadata metadata,
            Status s) {
        if (customIntervals == null)
            return Optional.empty();

        for (CustomInterval customInterval : customIntervals) {
            // an status had been set for this interval and it does not match
            // the one for this URL
            if (customInterval.status != null && customInterval.status != s) {
                continue;
            }
            String[] values = metadata.getValues(customInterval.key);
            if (values == null) {
                continue;
            }
            for (String v : values) {
                if (v.equals(customInterval.value)) {
                    return Optional.of(customInterval.minutes);
                }
            }
        }

        return Optional.empty();
    }

    private class CustomInterval {
        private String key;
        private String value;
        private Status status;
        private int minutes;

        private CustomInterval(String key, String value, Status status,
                int minutes) {
            this.key = key;
            this.value = value;
            this.status = status;
            this.minutes = minutes;
        }
    }
}
