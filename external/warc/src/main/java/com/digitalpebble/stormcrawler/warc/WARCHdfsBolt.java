package com.digitalpebble.stormcrawler.warc;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;

@SuppressWarnings("serial")
public class WARCHdfsBolt extends GzipHdfsBolt {

    private Map<String, String> header_fields;

    public WARCHdfsBolt() {
        super();
        FileSizeRotationPolicy rotpol = new FileSizeRotationPolicy(1.0f,
                Units.GB);
        withRecordFormat(new WARCRecordFormat()).withRotationPolicy(rotpol);
        // dummy sync policy
        withSyncPolicy(new CountSyncPolicy(10));
        // trigger rotation on size of compressed WARC file (not uncompressed
        // content)
        this.setRotationCompressedSizeOffset(true);
        // default local filesystem
        withFsUrl("file:///");
    }

    public WARCHdfsBolt withHeader(Map<String, String> header_fields) {
        this.header_fields = header_fields;
        return this;
    }

    protected Path createOutputFile() throws IOException {
        Path path = super.createOutputFile();

        Date now = new Date();

        // overrides the filename and creation date in the headers
        header_fields.put("WARC-Date", WARCRecordFormat.WARC_DF.format(now));
        header_fields.put("WARC-Filename", path.getName());

        byte[] header = WARCRecordFormat.generateWARCInfo(header_fields);

        // write the header at the beginning of the file
        if (header != null && header.length > 0) {
            writeRecord(header);
        }

        return path;
    }

}
