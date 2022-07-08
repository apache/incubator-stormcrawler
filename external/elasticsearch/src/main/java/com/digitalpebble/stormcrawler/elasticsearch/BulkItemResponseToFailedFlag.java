package com.digitalpebble.stormcrawler.elasticsearch;

import java.io.IOException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.jetbrains.annotations.NotNull;

public final class BulkItemResponseToFailedFlag {
    @NotNull public final BulkItemResponse response;
    public final boolean failed;
    @NotNull public final String id;

    public BulkItemResponseToFailedFlag(@NotNull BulkItemResponse response, boolean failed) {
        this.response = response;
        this.failed = failed;
        this.id = response.getId();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BulkItemResponseToFailedFlag)) return false;

        BulkItemResponseToFailedFlag that = (BulkItemResponseToFailedFlag) o;

        if (failed != that.failed) return false;
        if (!response.equals(that.response)) return false;
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        int result = response.hashCode();
        result = 31 * result + (failed ? 1 : 0);
        result = 31 * result + id.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "BulkItemResponseToFailedFlag{"
                + "response="
                + response
                + ", failed="
                + failed
                + ", id='"
                + id
                + '\''
                + '}';
    }

    public RestStatus status() {
        return response.status();
    }

    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params)
            throws IOException {
        return response.toXContent(builder, params);
    }

    public int getItemId() {
        return response.getItemId();
    }

    public DocWriteRequest.OpType getOpType() {
        return response.getOpType();
    }

    public String getIndex() {
        return response.getIndex();
    }

    public String getType() {
        return response.getType();
    }

    public long getVersion() {
        return response.getVersion();
    }

    public <T extends DocWriteResponse> T getResponse() {
        return response.getResponse();
    }

    public boolean isFailed() {
        return response.isFailed();
    }

    public String getFailureMessage() {
        return response.getFailureMessage();
    }

    public BulkItemResponse.Failure getFailure() {
        return response.getFailure();
    }

    public void writeTo(StreamOutput out) throws IOException {
        response.writeTo(out);
    }

    public void writeThin(StreamOutput out) throws IOException {
        response.writeThin(out);
    }

    public boolean isFragment() {
        return response.isFragment();
    }
}
