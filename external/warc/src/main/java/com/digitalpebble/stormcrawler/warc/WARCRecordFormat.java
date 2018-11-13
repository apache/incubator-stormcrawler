package com.digitalpebble.stormcrawler.warc;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.commons.codec.binary.Base32;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.protocol.HttpHeaders;

/** Generate a byte representation of a WARC entry from a tuple **/
@SuppressWarnings("serial")
public class WARCRecordFormat implements RecordFormat {

    private static final String WARC_VERSION = "WARC/1.0";
    private static final String CRLF = "\r\n";
    private static final byte[] CRLF_BYTES = { 13, 10 };

    private static final Logger LOG = LoggerFactory
            .getLogger(WARCRecordFormat.class);

    public static final SimpleDateFormat WARC_DF = new SimpleDateFormat(
            "yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ENGLISH);

    private static final Base32 base32 = new Base32();
    private static final String digestNoContent = getDigestSha1(new byte[0]);

    public static String getDigestSha1(byte[] bytes) {
        return "sha1:" + base32.encodeAsString(DigestUtils.sha1(bytes));
    }

    public static String getDigestSha1(byte[] bytes1, byte[] bytes2) {
        MessageDigest sha1 = DigestUtils.getSha1Digest();
        sha1.update(bytes1);
        return "sha1:" + base32.encodeAsString(sha1.digest(bytes2));
    }

    /**
     * Generates a WARC info entry which can be stored at the beginning of each
     * WARC file.
     **/
    public static byte[] generateWARCInfo(Map<String, String> fields) {
        StringBuffer buffer = new StringBuffer();
        buffer.append(WARC_VERSION);
        buffer.append(CRLF);

        buffer.append("WARC-Type: warcinfo").append(CRLF);

        String mainID = UUID.randomUUID().toString();

        // retrieve the date and filename from the map
        String date = fields.get("WARC-Date");
        buffer.append("WARC-Date: ").append(date).append(CRLF);

        String filename = fields.get("WARC-Filename");
        buffer.append("WARC-Filename: ").append(filename).append(CRLF);

        buffer.append("WARC-Record-ID").append(": ").append("<urn:uuid:")
                .append(mainID).append(">").append(CRLF);

        buffer.append("Content-Type").append(": ")
                .append("application/warc-fields").append(CRLF);

        StringBuilder fieldsBuffer = new StringBuilder();

        // add WARC fields
        // http://bibnum.bnf.fr/warc/WARC_ISO_28500_version1_latestdraft.pdf
        Iterator<Entry<String, String>> iter = fields.entrySet().iterator();
        while (iter.hasNext()) {
            Entry<String, String> entry = iter.next();
            String key = entry.getKey();
            if (key.startsWith("WARC-"))
                continue;
            fieldsBuffer.append(key).append(": ").append(entry.getValue())
                    .append(CRLF);
        }

        buffer.append("Content-Length")
                .append(": ")
                .append(fieldsBuffer.toString()
                        .getBytes(StandardCharsets.UTF_8).length).append(CRLF);

        buffer.append(CRLF);

        buffer.append(fieldsBuffer.toString());

        buffer.append(CRLF);
        buffer.append(CRLF);

        return buffer.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] format(Tuple tuple) {

        byte[] content = tuple.getBinaryByField("content");
        String url = tuple.getStringByField("url");
        Metadata metadata = (Metadata) tuple.getValueByField("metadata");

        // were the headers stored as is? Can write a response element then
        String headersVerbatim = metadata.getFirstValue("_response.headers_");
        byte[] httpheaders = new byte[0];
        if (StringUtils.isNotBlank(headersVerbatim)) {
            // check that ends with an empty line
            if (!headersVerbatim.endsWith(CRLF + CRLF)) {
                headersVerbatim += CRLF + CRLF;
            }
            httpheaders = headersVerbatim.getBytes();
        }

        StringBuilder buffer = new StringBuilder();
        buffer.append(WARC_VERSION);
        buffer.append(CRLF);

        String mainID = UUID.randomUUID().toString();

        buffer.append("WARC-Record-ID").append(": ").append("<urn:uuid:")
                .append(mainID).append(">").append(CRLF);

        int contentLength = 0;
        String payloadDigest = digestNoContent;
        String blockDigest;
        if (content != null) {
            contentLength = content.length;
            payloadDigest = getDigestSha1(content);
            blockDigest = getDigestSha1(httpheaders, content);
        } else {
            blockDigest = getDigestSha1(httpheaders);
        }

        // add the length of the http header
        contentLength += httpheaders.length;

        buffer.append("Content-Length").append(": ")
                .append(Integer.toString(contentLength)).append(CRLF);

        // TODO get actual fetch time from metadata if any
        Date now = new Date();
        buffer.append("WARC-Date").append(": ").append(WARC_DF.format(now))
                .append(CRLF);

        // check if http headers have been stored verbatim
        // if not generate a response instead
        String WARCTypeValue = "resource";

        if (StringUtils.isNotBlank(headersVerbatim)) {
            WARCTypeValue = "response";
        }

        buffer.append("WARC-Type").append(": ").append(WARCTypeValue)
                .append(CRLF);

        // "WARC-IP-Address" if present
        String IP = metadata.getFirstValue("_ip_");
        if (StringUtils.isNotBlank(IP)) {
            buffer.append("WARC-IP-Address").append(": ").append("IP")
                    .append(CRLF);
        }

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

        // provide a ContentType if type response
        if (WARCTypeValue.equals("response")) {
            buffer.append("Content-Type: application/http; msgtype=response")
                    .append(CRLF);
        }
        // for resources just use the content type provided by the server if any
        else {
            String ct = metadata.getFirstValue(HttpHeaders.CONTENT_TYPE);
            if (StringUtils.isBlank(ct)) {
                ct = "application/octet-stream";
            }
            buffer.append("Content-Type: ").append(ct).append(CRLF);
        }

        buffer.append("WARC-Payload-Digest").append(": ").append(payloadDigest)
                .append(CRLF);
        buffer.append("WARC-Block-Digest").append(": ").append(blockDigest)
                .append(CRLF);

        byte[] buffasbytes = buffer.toString().getBytes(StandardCharsets.UTF_8);

        // work out the *exact* length of the bytebuffer - do not add any extra
        // bytes which are appended as trailing zero bytes causing invalid WARC
        // files
        int capacity = 6 + buffasbytes.length + httpheaders.length;
        if (content != null) {
            capacity += content.length;
        }

        ByteBuffer bytebuffer = ByteBuffer.allocate(capacity);
        bytebuffer.put(buffasbytes);
        bytebuffer.put(CRLF_BYTES);
        bytebuffer.put(httpheaders);
        // the binary content itself
        if (content != null) {
            bytebuffer.put(content);
        }
        bytebuffer.put(CRLF_BYTES);
        bytebuffer.put(CRLF_BYTES);

        return bytebuffer.array();
    }

}
