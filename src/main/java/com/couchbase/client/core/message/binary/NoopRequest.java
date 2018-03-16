package com.couchbase.client.core.message.binary;

public class NoopRequest extends AbstractBinaryRequest implements BinaryRequest {

    public NoopRequest(String key, String bucket, String password) {
        super(key, bucket, password);
    }

    @Override
    public short partition() {
        return -1;
    }
}
