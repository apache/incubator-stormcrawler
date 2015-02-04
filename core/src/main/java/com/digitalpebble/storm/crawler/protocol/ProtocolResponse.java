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

package com.digitalpebble.storm.crawler.protocol;

import com.digitalpebble.storm.crawler.Metadata;

public class ProtocolResponse {

    private final byte[] content;
    private final int statusCode;
    private final Metadata metadata;

    public ProtocolResponse(byte[] c, int s, Metadata md) {
        content = c;
        statusCode = s;
        metadata = md == null ? new Metadata() : md;
    }

    public byte[] getContent() {
        return content;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public Metadata getMetadata() {
        return metadata;
    }

}
