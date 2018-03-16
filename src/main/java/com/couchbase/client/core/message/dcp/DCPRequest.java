package com.couchbase.client.core.message.dcp;

import com.couchbase.client.core.message.CouchbaseRequest;

/**
 * @author Sergey Avseyev
 * @since 1.0.2
 */
public interface DCPRequest extends CouchbaseRequest {
    /**
     * The partition (vbucket) to use for this request.
     *
     * @return the partition to use.
     */
    short partition();

    /**
     * Set the partition ID.
     *
     * @param id the id of the partition.
     * @return the {@link DCPRequest} for proper chaining.
     */
    DCPRequest partition(short id);
}
