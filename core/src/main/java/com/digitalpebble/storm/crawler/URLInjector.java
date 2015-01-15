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

package com.digitalpebble.storm.crawler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import com.digitalpebble.storm.fetchqueue.ShardedQueue;

/**
 * A simple client which puts URLS in a sharded queue so that they get processed
 * with the storm pipeline
 **/

public class URLInjector extends ConfigurableTopology {

    private ShardedQueue queue;

    public static void main(String[] args) throws Exception {
        ConfigurableTopology.start(new URLInjector(), args);
    }

    @Override
    protected int run(String[] args) {
        String messages = args[0];
        try {
            queue = ShardedQueue.getInstance(getConf());

            BufferedReader reader = new BufferedReader(new FileReader(new File(
                    messages)));
            String line = null;
            while ((line = reader.readLine()) != null) {
                queue.add(line.trim());
            }

            reader.close();
            queue.close();

            return 0;
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return -1;
        }
    }

}
