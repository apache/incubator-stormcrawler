/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.stormcrawler.warc;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.tuple.Tuple;
import org.apache.stormcrawler.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*

Example if metadata record
https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/#example-of-metadata-record

WARC/1.1
WARC-Type: metadata
WARC-Target-URI: http://www.archive.org/images/logoc.jpg
WARC-Date: 2016-09-19T17:20:24Z
WARC-Record-ID: <urn:uuid:16da6da0-bcdc-49c3-927e-57494593b943>
WARC-Concurrent-To: <urn:uuid:92283950-ef2f-4d72-b224-f54c6ec90bb0>
Content-Type: application/warc-fields
WARC-Block-Digest: sha1:VXT4AF5BBZVHDYKNC2CSM8TEAWDB6CH8
Content-Length: 59

via: http://www.archive.org/
hopsFromSeed: E
fetchTimeMs: 565
*/

public class MetadataRecordFormat extends WARCRecordFormat {

    private static final Logger LOG = LoggerFactory.getLogger(WARCRequestRecordFormat.class);

    private List<String> metadataKeys;

    public MetadataRecordFormat(List<String> metadataKeys) {
        super("");
        this.metadataKeys = metadataKeys;
    }

    @Override
    public byte[] format(Tuple tuple) {

        String url = tuple.getStringByField("url");
        Metadata metadata = (Metadata) tuple.getValueByField("metadata");

        final StringBuilder payload = new StringBuilder();

        // get the metadata key / values to save in the WARCs
        for (String k : metadataKeys) {
            for (String value : metadata.getValues(k)) {
                if (StringUtils.isBlank(value)) continue;
                payload.append(k).append(": ").append(value).append(CRLF);
            }
        }

        // no payload? don't bother
        if (payload.length() == 0) return new byte[0];

        final byte[] metadata_representation = payload.toString().getBytes(StandardCharsets.UTF_8);

        final StringBuilder buffer = new StringBuilder();
        buffer.append(WARC_VERSION);
        buffer.append(CRLF);

        buffer.append("WARC-Type: ").append(WARC_TYPE_METADATA).append(CRLF);

        final String mainID = UUID.randomUUID().toString();
        buffer.append("WARC-Record-ID: ")
                .append("<urn:uuid:")
                .append(mainID)
                .append(">")
                .append(CRLF);

        int contentLength = metadata_representation.length;
        buffer.append("Content-Length: ").append(Integer.toString(contentLength)).append(CRLF);

        String blockDigest = getDigestSha1(metadata_representation);

        String captureTime = getCaptureTime(metadata);
        buffer.append("WARC-Date: ").append(captureTime).append(CRLF);

        // must be a valid URI
        try {
            String normalised = url.replaceAll(" ", "%20");
            String targetURI = URI.create(normalised).toASCIIString();
            buffer.append("WARC-Target-URI: ").append(targetURI).append(CRLF);
        } catch (Exception e) {
            LOG.warn("Incorrect URI: {}", url);
            return new byte[] {};
        }

        buffer.append("Content-Type: application/warc-fields").append(CRLF);
        buffer.append("WARC-Block-Digest: ").append(blockDigest).append(CRLF);

        byte[] buffAsBytes = buffer.toString().getBytes(StandardCharsets.UTF_8);

        int capacity = 6 + buffAsBytes.length + metadata_representation.length;

        ByteBuffer bytebuffer = ByteBuffer.allocate(capacity);
        bytebuffer.put(buffAsBytes);
        bytebuffer.put(CRLF_BYTES);
        bytebuffer.put(metadata_representation);
        bytebuffer.put(CRLF_BYTES);
        bytebuffer.put(CRLF_BYTES);

        return bytebuffer.array();
    }
}
