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

import static com.digitalpebble.stormcrawler.protocol.ProtocolResponse.REQUEST_HEADERS_KEY;
import static com.digitalpebble.stormcrawler.protocol.ProtocolResponse.RESPONSE_IP_KEY;

/**
 * Generate a byte representation of a WARC request record from a tuple if the
 * request HTTP headers are present. The request record ID is stored in the
 * metadata so that a WARC response record (created later) can refer to it.
 **/
@SuppressWarnings("serial")
public class WARCRequestRecordFormat extends WARCRecordFormat {

    private static final Logger LOG = LoggerFactory
            .getLogger(WARCRequestRecordFormat.class);

    public WARCRequestRecordFormat(String protocolMDprefix) {
        super(protocolMDprefix);
    }

    @Override
    public byte[] format(Tuple tuple) {

        String url = tuple.getStringByField("url");
        Metadata metadata = (Metadata) tuple.getValueByField("metadata");

        String headersVerbatim = metadata.getFirstValue(REQUEST_HEADERS_KEY, this.protocolMDprefix);
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
        buffer.append("WARC-Type: ").append(WARC_TYPE_REQUEST).append(CRLF);

        // "WARC-IP-Address" if present
        String IP = metadata.getFirstValue(RESPONSE_IP_KEY, this.protocolMDprefix);
        if (StringUtils.isNotBlank(IP)) {
            buffer.append("WARC-IP-Address: ").append(IP).append(CRLF);
        }

        String mainID = UUID.randomUUID().toString();
        buffer.append("WARC-Record-ID: ").append("<urn:uuid:").append(mainID)
                .append(">").append(CRLF);
        /*
         * The request record ID is stored in the metadata so that a WARC
         * response record can later refer to it. Deactivated because of
         * https://github.com/DigitalPebble/storm-crawler/issues/721
         */
        // metadata.setValue("_request.warc_record_id_", mainID);

        int contentLength = httpheaders.length;
        buffer.append("Content-Length: ")
                .append(Integer.toString(contentLength)).append(CRLF);

        String blockDigest = getDigestSha1(httpheaders);

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

        buffer.append("Content-Type: application/http; msgtype=request")
                .append(CRLF);
        buffer.append("WARC-Block-Digest: ").append(blockDigest).append(CRLF);

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
