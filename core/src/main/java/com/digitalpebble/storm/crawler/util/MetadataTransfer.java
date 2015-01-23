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
	/**
	 * Parameter name indicating which metadata to transfer to the outlinks.
	 * Boolean value.
	 **/
	public static final String metadataTransferParamName = "metadata.transfer";

	/**
	 * Parameter name indicating whether to track the url path or not. Value is
	 * either a vector or a single valued String.
	 **/
	public static final String trackPathParamName = "metadata.track.path";

	/** Metadata key name for tracking the source URLs **/
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

	public Map<String, String[]> getMetaForOutlink(String sourceURL,
			Map<String, String[]> parentMD) {
		HashMap<String, String[]> md = new HashMap<String, String[]>();

		// what to keep from parentMD?
		for (String key : mdToKeep) {
			String[] vals = parentMD.get(key);
			if (vals != null)
				md.put(key, vals);
		}

		// keep the path?
		if (!trackPath)
			return md;

		// get any existing path from parent?
		String[] vals = parentMD.get(urlPathKeyName);
		if (vals == null)
			vals = new String[] { sourceURL };
		else {
			String[] newVals = new String[vals.length + 1];
			System.arraycopy(vals, 0, newVals, 0, newVals.length);
			vals = newVals;
		}
		md.put(urlPathKeyName, vals);

		return md;
	}
}
