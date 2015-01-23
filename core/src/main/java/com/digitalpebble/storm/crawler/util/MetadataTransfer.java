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
package com.digitalpebble.storm.crawler.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import clojure.lang.PersistentVector;

/**
 * Implements the logic of how the metadata should be passed to the outlinks,
 * what should be stored back in the persistence layer etc...
 **/
public class MetadataTransfer {

    private static String metadataTransferParamName = "metadata.transfer";

    private List<String> mdToKeep = new ArrayList<String>();

    public MetadataTransfer(Map<String, Object> conf) {
        Object obj = conf.get(metadataTransferParamName);
        if (obj == null)
            return;

        if (obj instanceof PersistentVector) {
            mdToKeep.addAll((PersistentVector) obj);
        }
        // single value?
        else {
            mdToKeep.add(obj.toString());
        }
    }

    public Map<String, String[]> getMetaForOutlink(
            Map<String, String[]> parentMD) {
        HashMap<String, String[]> md = new HashMap<String, String[]>();

        // what to keep from parentMD?
        for (String key : mdToKeep) {
            String[] vals = parentMD.get(key);
            if (vals != null)
                md.put(key, vals);
        }

        return md;
    }

}
