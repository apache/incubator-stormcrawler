package com.digitalpebble.stormcrawler.warc;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.common.AbstractHDFSWriter;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;

@SuppressWarnings("serial")
public class WARCHdfsBolt extends GzipHdfsBolt {

    private Map<String, String> header_fields = new HashMap<>();

    public WARCHdfsBolt() {
        super();
        FileSizeRotationPolicy rotpol = new FileSizeRotationPolicy(1.0f,
                Units.GB);
        withRecordFormat(new WARCRecordFormat());
        withRotationPolicy(rotpol);
        // dummy sync policy
        withSyncPolicy(new CountSyncPolicy(10));
        // default local filesystem
        withFsUrl("file:///");
    }

    public WARCHdfsBolt withHeader(Map<String, String> header_fields) {
        this.header_fields = header_fields;
        return this;
    }

    @Override
    protected AbstractHDFSWriter makeNewWriter(Path path, Tuple tuple)
            throws IOException {
        AbstractHDFSWriter writer = super.makeNewWriter(path, tuple);

        Date now = new Date();

        // overrides the filename and creation date in the headers
        header_fields.put("WARC-Date", WARCRecordFormat.WARC_DF.format(now));
        header_fields.put("WARC-Filename", path.getName());

        byte[] header = WARCRecordFormat.generateWARCInfo(header_fields);

        // write the header at the beginning of the file
        if (header != null && header.length > 0) {
            super.out.write(Utils.gzip(header));
        }

        return writer;
    }

}
