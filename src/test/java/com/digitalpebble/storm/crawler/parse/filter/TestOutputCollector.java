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

package com.digitalpebble.storm.crawler.parse.filter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import backtype.storm.task.IOutputCollector;
import backtype.storm.tuple.Tuple;

public class TestOutputCollector implements IOutputCollector {
    private List<Tuple> acked = new ArrayList<>();
    private List<Tuple> failed = new ArrayList<>();
    private List<List<Object>> emitted = new ArrayList<>();

    @Override
    public void reportError(Throwable error) {}

    @Override
    public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuples) {
        emitted.add(tuples);
        // No idea what to return
        return null;
    }

    @Override
    public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors,
            List<Object> tuple) {}

    @Override
    public void ack(Tuple input) {
        acked.add(input);
    }

    @Override
    public void fail(Tuple input) {
        failed.add(input);
    }

    public List<List<Object>> getEmitted() {
        return emitted;
    }

    public List<Tuple> getAckedTuples() {
        return acked;
    }

    public List<Tuple> getFailedTuples() {
        return failed;
    }

}
