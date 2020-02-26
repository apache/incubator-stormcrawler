package com.digitalpebble.stormcrawler.warc;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.protocol.ProtocolResponse;

/**
 * Generate a byte representation of a WARC request record from a tuple if the
 * request HTTP headers are present. The request record ID is stored in the
 * metadata so that a WARC response record (created later) can refer to it.
 **/
@SuppressWarnings("serial")
public class WARCRequestRecordFormat extends WARCRecordFormat {

    private static final Logger LOG = LoggerFactory
            .getLogger(WARCRequestRecordFormat.class);

    private final String requestHeaderKey;

    public WARCRequestRecordFormat(String protocolMDprefix) {
        super(protocolMDprefix);
        requestHeaderKey = protocolMDprefix
                + ProtocolResponse.REQUEST_HEADERS_KEY;
    }

    @Override
    public byte[] format(Tuple tuple) {

        String url = tuple.getStringByField("url");
        Metadata metadata = (Metadata) tuple.getValueByField("metadata");

        String headersVerbatim = metadata.getFirstValue(requestHeaderKey);
        byte[] httpheaders = new byte[0];
        if (StringUtils.isBlank(headersVerbatim)) {
            // no request header: return empty record
            LOG.warn("No request header for {}", url);
            return new byte[] {};
        } else {
            // check that header ends with an empty line
            while (!headersVerbatim.endsWith(CRLF + CRLF)) {
                headersVerbatim += CRLF;
            }
            httpheaders = headersVerbatim.getBytes();
        }

        StringBuilder buffer = new StringBuilder();
        buffer.append(WARC_VERSION);
        buffer.append(CRLF);
        buffer.append("WARC-Type").append(": ").append("request").append(CRLF);

        // "WARC-IP-Address" if present
        String IP = metadata.getFirstValue(responseIPKey);
        if (StringUtils.isNotBlank(IP)) {
            buffer.append("WARC-IP-Address").append(": ").append(IP)
                    .append(CRLF);
        }

        String mainID = UUID.randomUUID().toString();
        buffer.append("WARC-Record-ID").append(": ").append("<urn:uuid:")
                .append(mainID).append(">").append(CRLF);
        /*
         * The request record ID is stored in the metadata so that a WARC
         * response record can later refer to it. Deactivated because of
         * https://github.com/DigitalPebble/storm-crawler/issues/721
         */
        // metadata.setValue("_request.warc_record_id_", mainID);

        int contentLength = httpheaders.length;
        buffer.append("Content-Length").append(": ")
                .append(Integer.toString(contentLength)).append(CRLF);

        String blockDigest = getDigestSha1(httpheaders);

        String captureTime = getCaptureTime(metadata);
        buffer.append("WARC-Date").append(": ").append(captureTime)
                .append(CRLF);

        // must be a valid URI
        try {
            String normalised = url.replaceAll(" ", "%20");
            String targetURI = URI.create(normalised).toASCIIString();
            buffer.append("WARC-Target-URI").append(": ").append(targetURI)
                    .append(CRLF);
        } catch (Exception e) {
            LOG.warn("Incorrect URI: {}", url);
            return new byte[] {};
        }

        buffer.append("Content-Type: application/http; msgtype=request")
                .append(CRLF);
        buffer.append("WARC-Block-Digest").append(": ").append(blockDigest)
                .append(CRLF);

        byte[] buffasbytes = buffer.toString().getBytes(StandardCharsets.UTF_8);

        int capacity = 6 + buffasbytes.length + httpheaders.length;

        ByteBuffer bytebuffer = ByteBuffer.allocate(capacity);
        bytebuffer.put(buffasbytes);
        bytebuffer.put(CRLF_BYTES);
        bytebuffer.put(httpheaders);
        bytebuffer.put(CRLF_BYTES);
        bytebuffer.put(CRLF_BYTES);

        return bytebuffer.array();
    }

}
