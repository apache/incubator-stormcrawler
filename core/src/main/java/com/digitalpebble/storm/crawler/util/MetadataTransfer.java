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

import org.apache.commons.lang.StringUtils;

import clojure.lang.PersistentVector;

import com.digitalpebble.storm.crawler.Metadata;

/**
 * Implements the logic of how the metadata should be passed to the outlinks,
 * what should be stored back in the persistence layer etc...
 */
public class MetadataTransfer {

    /**
     * Class to use for transfering metadata to outlinks. Must extend the class
     * MetadataTransfer.
     */
    public static final String metadataTransferClassParamName = "metadata.transfer.class";

    /**
     * Parameter name indicating which metadata to transfer to the outlinks.
     * Value is either a vector or a single valued String.
     */
    public static final String metadataTransferParamName = "metadata.transfer";

    /**
     * Parameter name indicating whether to track the url path or not. Boolean
     * value, true by default.
     */
    public static final String trackPathParamName = "metadata.track.path";

    /**
     * Parameter name indicating whether to track the depth from seed. Boolean
     * value, true by default.
     */
    public static final String trackDepthParamName = "metadata.track.depth";

    /** Metadata key name for tracking the source URLs */
    public static final String urlPathKeyName = "url.path";

    /** Metadata key name for tracking the depth */
    public static final String depthKeyName = "depth";

    private List<String> mdToKeep = new ArrayList<String>();

    private boolean trackPath = true;

    private boolean trackDepth = true;

    public static MetadataTransfer getInstance(Map<String, Object> conf) {
        String className = ConfUtils.getString(conf,
                metadataTransferClassParamName);

        MetadataTransfer transferInstance;

        // no custom class specified
        if (StringUtils.isBlank(className)) {
            transferInstance = new MetadataTransfer();
        }

        else {
            try {
                Class<?> transferClass = Class.forName(className);
                boolean interfaceOK = MetadataTransfer.class
                        .isAssignableFrom(transferClass);
                if (!interfaceOK) {
                    throw new RuntimeException("Class " + className
                            + " must extend MetadataTransfer");
                }
                transferInstance = (MetadataTransfer) transferClass
                        .newInstance();
            } catch (Exception e) {
                throw new RuntimeException("Can't instanciate " + className);
            }
        }

        // should not be null
        if (transferInstance != null)
            transferInstance.configure(conf);

        return transferInstance;
    }

    protected void configure(Map<String, Object> conf) {

        trackPath = ConfUtils.getBoolean(conf, trackPathParamName, true);

        trackDepth = ConfUtils.getBoolean(conf, trackDepthParamName, true);

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
    public Metadata getMetaForOutlink(String targetURL, String sourceURL,
            Metadata parentMD) {
        Metadata md = filter(parentMD);

        // keep the path?
        if (trackPath) {
            md.addValue(urlPathKeyName, sourceURL);
        }

        // track depth
        if (trackDepth) {
            String existingDepth = md.getFirstValue(depthKeyName);
            int depth = 0;
            try {
                depth = Integer.parseInt(existingDepth);
            } catch (Exception e) {
                depth = 0;
            }
            md.setValue(depthKeyName, Integer.toString(++depth));
        }

        return md;
    }

    /**
     * Determine which metadata should be kept e.g. for storing into a database
     **/
    public Metadata filter(Metadata metadata) {
        Metadata md = new Metadata();

        List<String> metadataToKeep = new ArrayList<String>(mdToKeep.size());
        metadataToKeep.addAll(mdToKeep);

        // keep the path but don't add anything to it
        if (trackPath) {
            metadataToKeep.add(urlPathKeyName);
        }

        // keep the depth but don't add anything to it
        if (trackDepth) {
            metadataToKeep.add(depthKeyName);
        }

        // what to keep from parentMD?
        for (String key : metadataToKeep) {
            String[] vals = metadata.getValues(key);
            if (vals != null)
                md.setValues(key, vals);
        }

        return md;
    }
}
