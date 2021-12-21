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
package com.digitalpebble.stormcrawler.persistence;

import com.digitalpebble.stormcrawler.Metadata;
import java.util.Date;
import java.util.Optional;
import org.apache.storm.tuple.Tuple;

/**
 * Dummy status updater which dumps the content of the incoming tuples to the standard output.
 * Useful for debugging and as an illustration of what AbstractStatusUpdaterBolt provides.
 */
public class StdOutStatusUpdater extends AbstractStatusUpdaterBolt {

    @Override
    public void store(
            String url, Status status, Metadata metadata, Optional<Date> nextFetch, Tuple t)
            throws Exception {
        String nextFetchS = "NEVER";
        if (nextFetch.isPresent()) {
            nextFetchS = nextFetch.get().toString();
        }
        System.out.println(url + "\t" + status + "\t" + nextFetchS);
        System.out.println(metadata.toString("\t"));
        super.ack(t, url);
    }
}
