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

import java.util.Date;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.spout.MemorySpout;

/**
 * Use in combination with the MemorySpout for testing in local mode. There is
 * no guarantee that this will work in distributed mode as it expects the
 * MemorySpout to be in the same execution thread.
 **/
@SuppressWarnings("serial")
public class MemoryStatusUpdater extends AbstractStatusUpdaterBolt {

    @Override
    public void store(String url, Status status, Metadata metadata,
            Date nextFetch) throws Exception {
        // by convention we do not refetch URLs with a next fetch date of EPOCH
        if (nextFetch.equals(DefaultScheduler.NEVER))
            return;

        MemorySpout.add(url, metadata, nextFetch);
    }

}
