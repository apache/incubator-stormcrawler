/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

public abstract class ConfigurableTopology {

    public static void start(ConfigurableTopology topology, String args[]) {
        String[] remainingArgs = topology.parse(args);
        topology.run(remainingArgs);
    }

    protected Config conf = new Config();

    protected boolean isLocal = false;

    protected Config getConf() {
        return conf;
    }

    protected abstract int run(String args[]);

    protected int submit(String name, Config conf, TopologyBuilder builder) {
        if (isLocal) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(name, conf, builder.createTopology());
        }

        else {
            try {
                StormSubmitter.submitTopology(name, conf,
                        builder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
                return -1;
            }
        }
        return 0;
    }

    private String[] parse(String args[]) {

        List<String> newArgs = new ArrayList<String>();
        Collections.addAll(newArgs, args);

        Iterator<String> iter = newArgs.iterator();
        while (iter.hasNext()) {
            String param = iter.next();
            if (param.equals("-conf")) {
                if (!iter.hasNext()) {
                    System.err.println("Missing conf file");
                    System.exit(-1);
                }
                iter.remove();
                String resource = iter.next();
                Yaml yaml = new Yaml();
                Map ret = null;
                try {
                    ret = (Map) yaml.load(new InputStreamReader(
                            new FileInputStream(resource)));
                } catch (FileNotFoundException e) {
                    System.err
                            .println("Conf file does not exist : " + resource);
                    System.exit(-1);
                }
                if (ret == null)
                    ret = new HashMap();
                conf.putAll(ret);
                iter.remove();
            } else if (param.equals("-local")) {
                isLocal = true;
                iter.remove();
            }
        }

        return (String[]) newArgs.toArray(new String[newArgs.size()]);
    }

}
