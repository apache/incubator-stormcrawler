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
import java.util.List;
import java.util.Map;

import clojure.lang.PersistentVector;

import com.digitalpebble.storm.crawler.Metadata;

/**
 * Implements the logic of how the metadata should be passed to the outlinks,
 * what should be stored back in the persistence layer etc...
 */
public class MetadataTransfer {
    /**
     * Parameter name indicating which metadata to transfer to the outlinks.
     * Boolean value.
     */
    public static final String metadataTransferParamName = "metadata.transfer";

    /**
     * Parameter name indicating whether to track the url path or not. Value is
     * either a vector or a single valued String.
     */
    public static final String trackPathParamName = "metadata.track.path";

    /** Metadata key name for tracking the source URLs */
    public static final String urlPathKeyName = "url.path";

    private List<String> mdToKeep = new ArrayList<String>();

    private boolean trackPath = true;

    public MetadataTransfer(Map<String, Object> conf) {

        trackPath = ConfUtils.getBoolean(conf, trackPathParamName, true);

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

    /**
     * Determine which metadata should be transfered to an outlink. Adds
     * additional metadata like the URL path.
     **/
    public Metadata getMetaForOutlink(String sourceURL, Metadata parentMD) {
        Metadata md = filter(parentMD);

        // keep the path?
        if (!trackPath) {
            md.addValue(urlPathKeyName, sourceURL);
        }

        return md;
    }

    /**
     * Determine which metadata should be kept e.g. for storing into a database
     **/
    public Metadata filter(Metadata parentMD) {
        Metadata md = new Metadata();

        List<String> copyMD = new ArrayList<String>(mdToKeep.size());
        copyMD.addAll(mdToKeep);

        // keep the path but don't add anything to it
        if (trackPath) {
            copyMD.add(urlPathKeyName);
        }

        // what to keep from parentMD?
        for (String key : copyMD) {
            String[] vals = parentMD.getValues(key);
            if (vals != null)
                md.setValues(key, vals);
        }

        return md;
    }
}
