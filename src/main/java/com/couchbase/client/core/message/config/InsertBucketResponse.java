package com.couchbase.client.core.message.config;

import com.couchbase.client.core.message.AbstractCouchbaseResponse;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.ResponseStatus;

public class InsertBucketResponse extends AbstractCouchbaseResponse {

    public InsertBucketResponse(ResponseStatus status) {
        super(status, null);
    }
}
