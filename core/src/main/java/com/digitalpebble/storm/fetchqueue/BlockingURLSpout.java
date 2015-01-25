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

package com.digitalpebble.storm.fetchqueue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseComponent;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import com.digitalpebble.storm.crawler.Constants;
import com.digitalpebble.storm.crawler.util.ConfUtils;

/**
 * Reads from a sharded queue and blocks based on the number of un-acked URLs
 * per queue
 */
@SuppressWarnings("serial")
public class BlockingURLSpout extends BaseComponent implements IRichSpout {

    private static final Logger LOG = LoggerFactory
            .getLogger(BlockingURLSpout.class);

    private ShardedQueue queue;

    private SpoutOutputCollector collector;

    private Map<String, Integer> messageIDToQueueNum = new HashMap<String, Integer>();

    private int maxLiveURLsPerQueue;

    private int sleepTime;

    private List<LinkedBlockingQueue<Message>> queues;

    private int currentQueue = -1;

    private AtomicInteger[] queueCounter;

    private AtomicInteger totalProcessing = new AtomicInteger(0);

    @Override
    public void open(Map conf, TopologyContext context,
            SpoutOutputCollector collector) {

        this.collector = collector;

        maxLiveURLsPerQueue = ConfUtils.getInt(conf,
                Constants.maxLiveURLsPerQueueParamName, 10);

        sleepTime = ConfUtils.getInt(conf, Constants.keySleepTimeParamName, 50);

        try {
            queue = ShardedQueue.getInstance(conf);
        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw new RuntimeException(e);
        }

        int numQueues = queue.getNumShards();

        queues = new ArrayList<LinkedBlockingQueue<Message>>(numQueues);
        queueCounter = new AtomicInteger[numQueues];

        for (int i = 0; i < numQueues; i++) {
            queues.add(new LinkedBlockingQueue<Message>());
            queueCounter[i] = new AtomicInteger(0);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url"));
    }

    @Override
    public void close() {
        queue.close();
    }

    @Override
    public void ack(Object msgId) {
        int queueNumber = messageIDToQueueNum.remove(msgId);
        queueCounter[queueNumber].decrementAndGet();
        totalProcessing.decrementAndGet();
        queue.deleteMessage(queueNumber, msgId.toString());
    }

    @Override
    public void fail(Object msgId) {
        int queueNumber = messageIDToQueueNum.get(msgId);
        queueCounter[queueNumber].decrementAndGet();
        totalProcessing.decrementAndGet();
        queue.releaseMessage(queueNumber, msgId.toString());
    }

    @Override
    public void nextTuple() {
        // try to get a message from the current queue

        // all queues blocked?
        // take a break
        if (totalProcessing.get() == this.maxLiveURLsPerQueue
                * this.queueCounter.length) {
            Utils.sleep(sleepTime);
            return;
        }

        ++currentQueue;

        if (currentQueue == queues.size())
            currentQueue = 0;

        // how many items are alive for this queue?
        if (queueCounter[currentQueue].get() >= this.maxLiveURLsPerQueue) {
            return;
        }

        LinkedBlockingQueue<Message> currentQ = queues.get(currentQueue);
        boolean empty = currentQ.isEmpty();
        if (empty) {
            queue.fillQueue(currentQueue, currentQ);
        }

        Message message = currentQ.poll();
        if (message == null)
            return;

        queueCounter[currentQueue].incrementAndGet();
        totalProcessing.incrementAndGet();

        // finally we got a message
        // its content is a URL
        List<Object> tuple = new ArrayList<Object>();
        tuple.add(message.getContent());

        messageIDToQueueNum.put(message.getId(), currentQueue);

        collector.emit(Utils.DEFAULT_STREAM_ID, tuple, message.getId());

        return;
    }

    @Override
    public void activate() {
        // TODO Auto-generated method stub

    }

    @Override
    public void deactivate() {
        // TODO Auto-generated method stub

    }

}
