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

package com.digitalpebble.stormcrawler.elasticsearch.metrics;

import java.text.DateFormat;
import java.util.Date;

/**
 * Elasticsearch metrics consumer that writes an index per time period. The time
 * period is specified by a date formatter supplied by concrete subclasses.
 * <p>
 * This addresses the deprecation of the TTL functionality. The per-day indices
 * can be managed using ES Curator.
 */
abstract class IndexPerPeriodMetricsConsumer extends MetricsConsumer {

    /**
     * Date format used to append current time 'period' to the base metrics
     * index for per-time period indexing.
     * 
     * @return time period date formatter, e.g. "yyyy-MM-dd" for index per day
     */
    protected abstract DateFormat getDateFormat();

    @Override
    protected final String getIndexName() {
        return new StringBuilder().append(super.getIndexName()).append("-")
                .append(getDateFormat().format(new Date())).toString();
    }
}
