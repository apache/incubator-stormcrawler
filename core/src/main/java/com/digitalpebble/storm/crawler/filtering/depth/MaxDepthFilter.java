package com.digitalpebble.storm.crawler.filtering.depth;

import java.net.URL;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.storm.crawler.Metadata;
import com.digitalpebble.storm.crawler.filtering.URLFilter;
import com.digitalpebble.storm.crawler.util.MetadataTransfer;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Filter out URLs whose depth is greater than maxDepth.
 */
public class MaxDepthFilter implements URLFilter {

    private static final Logger LOG = LoggerFactory.getLogger(MaxDepthFilter.class);

    private int maxDepth;

    @Override
    public void configure(Map stormConf, JsonNode paramNode) {
        JsonNode node = paramNode.get("maxDepth");
        if (node != null && node.isInt()) {
            maxDepth = node.intValue();
        } else {
            maxDepth = 0;
            LOG.warn("maxDepth paramater not found");
        }
    }

    @Override
    public String filter(URL pageUrl, Metadata sourceMetadata, String url) {
        int depth = getDepth(sourceMetadata);
        if (maxDepth > 0 && depth > maxDepth) {
            return null;
        }
        return url;
    }

    private int getDepth(Metadata sourceMetadata) {
        String depth = sourceMetadata.getFirstValue(MetadataTransfer.depthKeyName);
        if (StringUtils.isNumeric(depth)) {
            return Integer.parseInt(depth);
        } else {
            return -1;
        }
    }
}
