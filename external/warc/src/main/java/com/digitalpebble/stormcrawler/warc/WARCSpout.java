package com.digitalpebble.stormcrawler.warc;

import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.netpreserve.jwarc.HttpMessage;
import org.netpreserve.jwarc.HttpRequest;
import org.netpreserve.jwarc.HttpResponse;
import org.netpreserve.jwarc.MediaType;
import org.netpreserve.jwarc.MessageBody;
import org.netpreserve.jwarc.WarcPayload;
import org.netpreserve.jwarc.WarcReader;
import org.netpreserve.jwarc.WarcRecord;
import org.netpreserve.jwarc.WarcRequest;
import org.netpreserve.jwarc.WarcResponse;
import org.netpreserve.jwarc.WarcTruncationReason;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.Constants;
import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.persistence.Status;
import com.digitalpebble.stormcrawler.protocol.ProtocolResponse;
import com.digitalpebble.stormcrawler.protocol.ProtocolResponse.TrimmedContentReason;
import com.digitalpebble.stormcrawler.spout.FileSpout;
import com.digitalpebble.stormcrawler.util.ConfUtils;

/**
 * Read WARC files from the local files system and emit the WARC captures as
 * tuples into the topology same ways as done by
 * {@link com.digitalpebble.stormcrawler.bolt.FetcherBolt}.
 */
@SuppressWarnings("serial")
public class WARCSpout extends FileSpout {

    private static final Logger LOG = LoggerFactory.getLogger(WARCSpout.class);

    private int maxContentSize = -1;
    private int contentBufferSize = 8192;

    private boolean storeHTTPHeaders = false;
    private String protocolMDprefix = "";

    private WarcReader warcReader;
    private String warcFileInProgress;
    private WarcRequest precedingWarcRequest;

    private MultiCountMetric eventCounter;

    public WARCSpout(String... files) {
        super(false, files);
    }

    public WARCSpout(String dir, String filter) {
        super(dir, filter, false);
    }

    public static class RecordHolder {
        WarcRecord record;
        byte[] content;

        public RecordHolder(WarcRecord record, byte[] content) {
            this.record = record;
            this.content = content;
        }
    }

    /**
     * Holder of truncation status when WARC payload exceeding the content
     * length limit (http.content.limit) is truncated.
     */
    public static class TruncationStatus {
        boolean isTruncated = false;
        int originalSize = -1;

        public void set(boolean isTruncated) {
            this.isTruncated = isTruncated;
        }

        public boolean get() {
            return isTruncated;
        }

        public void setOriginalSize(int size) {
            originalSize = size;
        }

        public int getOriginalSize() {
            return originalSize;
        }
    }

    private void openWARC() {
        if (warcReader != null) {
            try {
                warcReader.close();
            } catch (IOException e) {
                LOG.warn("Failed to close open WARC file", e);
            }
            warcReader = null;
        }

        byte[] head = buffer.removeFirst();
        List<Object> fields = _scheme.deserialize(ByteBuffer.wrap(head));
        warcFileInProgress = (String) fields.get(0);
        if (warcFileInProgress == null)
            return;

        LOG.info("Reading WARC file {}", warcFileInProgress);
        ReadableByteChannel warcChannel = null;
        try {
            if (warcFileInProgress.matches("^https?://.*")) {
                URL warcUrl = new URL(warcFileInProgress);
                warcChannel = Channels.newChannel(warcUrl.openStream());
            } else {
                warcChannel = FileChannel.open(Paths.get(warcFileInProgress));
            }
            warcReader = new WarcReader(warcChannel);
        } catch (IOException e) {
            LOG.error("Failed to open WARC file " + warcFileInProgress, e);
            warcFileInProgress = null;
            if (warcChannel != null) {
                try {
                    warcChannel.close();
                } catch (IOException ex) {
                }
            }
        }
    }

    private Optional<WarcRecord> nextWarcResponseRecord() {
        Optional<WarcRecord> record = Optional.empty();
        while (true) {
            while (warcReader == null && !buffer.isEmpty()) {
                openWARC();
            }
            if (warcReader == null) {
                // failed to open any new WARC file
                return record;
            }
            try {
                record = warcReader.next();
            } catch (IOException e) {
                LOG.error("Failed to read WARC " + warcFileInProgress, e);
            }
            if (record.isPresent()) {
                if (record.get() instanceof WarcResponse && record.get()
                        .contentType().equals(MediaType.HTTP_RESPONSE)) {
                    eventCounter.scope("warc_http_response_record").incr();
                    break; // got a response record
                } else {
                    String warcType = record.get().type();
                    if (warcType == null) {
                        LOG.warn("No type for {}", record.get().getClass());
                    } else {
                        eventCounter.scope("warc_skipped_record_of_type_"
                                + record.get().type()).incr();
                        LOG.debug("Skipped WARC record of type {}",
                                record.get().type());
                    }
                    if (this.storeHTTPHeaders && "request".equals(warcType)) {
                        // store request records to be able to add HTTP request
                        // header to metadata
                        precedingWarcRequest = (WarcRequest) record.get();
                        try {
                            // need to read and parse HTTP header right now
                            // (otherwise it's skipped)
                            precedingWarcRequest.http();
                        } catch (IOException e) {
                            LOG.error(
                                    "Failed to read HTTP request for {} in {}: {}",
                                    precedingWarcRequest.target(),
                                    warcFileInProgress, e);
                        }
                    }
                }
            } else {
                try {
                    warcReader.close();
                } catch (IOException e) {
                    LOG.warn("Failed to close WARC reader", e);
                }
                warcReader = null;
            }
        }
        return record;
    }

    private byte[] getContent(WarcResponse record, TruncationStatus isTruncated)
            throws IOException {
        Optional<WarcPayload> payload = record.payload();
        if (!payload.isPresent()) {
            return new byte[0];
        }
        MessageBody body = payload.get().body();
        long size = body.size();
        isTruncated.set(false);
        if (size > maxContentSize) {
            LOG.info(
                    "WARC payload of size {} to be truncated to {} bytes for {}",
                    size, maxContentSize, record.target());
            size = maxContentSize;
        }
        ByteBuffer buf;
        if (size >= 0) {
            buf = ByteBuffer.allocate((int) size);
        } else {
            buf = ByteBuffer.allocate(contentBufferSize);
        }
        // dynamically growing list of buffers for large content of unknown size
        ArrayList<ByteBuffer> bufs = new ArrayList<>();
        int r, read = 0;
        while (read < maxContentSize) {
            try {
                if ((r = body.read(buf)) < 0)
                    break; // eof
            } catch (Exception e) {
                LOG.error("Failed to read content of {}: {}", record.target(),
                        e);
                break;
            }
            if (r == 0 && !buf.hasRemaining()) {
                buf.flip();
                bufs.add(buf);
                buf = ByteBuffer.allocate(contentBufferSize);
            }
            read += r;
        }
        buf.flip();
        if (read == maxContentSize) {
            // to mark truncation: check whether there is more content
            r = body.read(ByteBuffer.allocate(1));
            if (r > -1) {
                isTruncated.set(true);
                // read remaining body (also to figure out original length)
                int truncatedLength = r;
                ByteBuffer buffer = ByteBuffer.allocate(8192);
                try {
                    while ((r = body.read(buffer)) >= 0) {
                        buffer.clear();
                        truncatedLength += r;
                    }
                    isTruncated.setOriginalSize(read + truncatedLength);
                } catch (IOException e) {
                    // ignore, it's about unused content
                }
                LOG.info(
                        "WARC payload of size {} is truncated to {} bytes for {}",
                        isTruncated.getOriginalSize(), maxContentSize,
                        record.target());
                body.consume();
            }
        }
        if (read == size) {
            // short-cut: return buffer-internal array
            return buf.array();
        }
        // copy buffers into result byte[]
        byte[] arr = new byte[read];
        int pos = 0;
        for (ByteBuffer b : bufs) {
            r = b.remaining();
            b.get(arr, pos, r);
            pos += r;
        }
        buf.get(arr, pos, buf.remaining());
        return arr;
    }

    private static String httpHeadersVerbatim(HttpMessage http) {
        return new String(http.serializeHeader(), StandardCharsets.UTF_8);
    }

    private void addVerbatimHttpHeaders(Metadata metadata,
            WarcResponse response, HttpResponse http, HttpRequest request) {
        metadata.addValue(protocolMDprefix + ProtocolResponse.REQUEST_TIME_KEY,
                Long.toString(response.date().toEpochMilli()));
        if (response.ipAddress().isPresent()) {
            metadata.addValue(
                    protocolMDprefix + ProtocolResponse.RESPONSE_IP_KEY,
                    response.ipAddress().get().getHostAddress());
        }
        if (request != null) {
            metadata.addValue(
                    protocolMDprefix + ProtocolResponse.REQUEST_HEADERS_KEY,
                    httpHeadersVerbatim(request));
        }
        metadata.addValue(
                protocolMDprefix + ProtocolResponse.RESPONSE_HEADERS_KEY,
                httpHeadersVerbatim(http));
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void open(Map conf, TopologyContext context,
            SpoutOutputCollector collector) {
        _collector = collector;
        maxContentSize = ConfUtils.getInt(conf, "http.content.limit", -1);
        if (contentBufferSize > maxContentSize) {
            // no need to buffer more content than max. used
            contentBufferSize = maxContentSize;
        }
        storeHTTPHeaders = ConfUtils.getBoolean(conf, "http.store.headers",
                false);
        protocolMDprefix = ConfUtils.getString(conf,
                ProtocolResponse.PROTOCOL_MD_PREFIX_PARAM, protocolMDprefix);
        int metricsTimeBucketSecs = ConfUtils.getInt(conf,
                "fetcher.metrics.time.bucket.secs", 10);
        eventCounter = context.registerMetric("warc_spout_counter",
                new MultiCountMetric(), metricsTimeBucketSecs);
    }

    @Override
    public void nextTuple() {
        if (!active)
            return;

        if (buffer.isEmpty()) {
            try {
                populateBuffer();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        if (warcReader == null && buffer.isEmpty()) {
            // input exhausted
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
            }
            return;
        }

        Optional<WarcRecord> record = nextWarcResponseRecord();
        if (!record.isPresent())
            return;

        WarcResponse w = (WarcResponse) record.get();

        String url = w.target();
        HttpResponse http;
        try {
            http = w.http();
        } catch (IOException e) {
            LOG.error("Failed to read HTTP response for {} in {}: {}", url,
                    warcFileInProgress, e);
            return;
        }
        LOG.info("Fetched {} with status {}", url, http.status());
        eventCounter.scope("fetched").incrBy(1);

        final Status status = Status.fromHTTPCode(http.status());
        eventCounter.scope("status_" + http.status()).incrBy(1);

        Metadata metadata = new Metadata();

        // Add HTTP response headers to metadata
        for (Map.Entry<String, List<String>> e : http.headers().map()
                .entrySet()) {
            metadata.addValues(protocolMDprefix + e.getKey(), e.getValue());
        }
        if (storeHTTPHeaders) {
            // if recording HTTP headers: add IP address, fetch date time,
            // literal request and response headers
            HttpRequest req = null;
            if (precedingWarcRequest != null && (w.concurrentTo()
                    .contains(precedingWarcRequest.id())
                    || w.target().equals(precedingWarcRequest.target()))) {
                try {
                    req = precedingWarcRequest.http();
                } catch (IOException e) {
                }
            }
            addVerbatimHttpHeaders(metadata, w, http, req);
        }

        if (status == Status.FETCHED && http.status() != 304) {
            byte[] content;
            TruncationStatus isTruncated = new TruncationStatus();
            try {
                content = getContent(w, isTruncated);
            } catch (IOException e) {
                LOG.error("Failed to read payload for {} in {}: {}", url,
                        warcFileInProgress, e);
                content = new byte[0];
            }
            eventCounter.scope("bytes_fetched").incrBy(content.length);
            if (isTruncated.get()
                    || w.truncated() != WarcTruncationReason.NOT_TRUNCATED) {
                WarcTruncationReason reason = WarcTruncationReason.LENGTH;
                if (w.truncated() != WarcTruncationReason.NOT_TRUNCATED) {
                    reason = w.truncated();
                }
                metadata.setValue(
                        protocolMDprefix
                                + ProtocolResponse.TRIMMED_RESPONSE_KEY,
                        "true");
                metadata.setValue(
                        protocolMDprefix
                                + ProtocolResponse.TRIMMED_RESPONSE_REASON_KEY,
                        reason.toString().toLowerCase(Locale.ROOT));
            }
            _collector.emit(new Values(url, content, metadata));
            return;
        }

        // redirects, 404s, etc.
        _collector.emit(Constants.StatusStreamName,
                new Values(url, metadata, status), url);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(Constants.StatusStreamName,
                new Fields("url", "metadata", "status"));
        declarer.declare(new Fields("url", "content", "metadata"));
    }

}
