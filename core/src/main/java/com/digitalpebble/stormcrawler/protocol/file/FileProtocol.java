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
package com.digitalpebble.stormcrawler.protocol.file;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.protocol.Protocol;
import com.digitalpebble.stormcrawler.protocol.ProtocolResponse;
import com.digitalpebble.stormcrawler.protocol.RobotRulesParser;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import crawlercommons.robots.BaseRobotRules;
import org.apache.storm.Config;

public class FileProtocol implements Protocol {

    private String encoding;

    @Override
    public void configure(Config conf) {
        encoding = ConfUtils.getString(conf, "file.encoding", "UTF-8");
    }

    @Override
    public ProtocolResponse getProtocolOutput(String url, Metadata md) throws Exception {
        FileResponse response = new FileResponse(url, md, this);
        return response.toProtocolResponse();
    }

    @Override
    public BaseRobotRules getRobotRules(String url) {
        return RobotRulesParser.EMPTY_RULES;
    }

    public String getEncoding() {
        return encoding;
    }

    @Override
    public void cleanup() {}
}
