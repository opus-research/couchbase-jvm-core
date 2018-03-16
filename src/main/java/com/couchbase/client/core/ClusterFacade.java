/**
 * Copyright (C) 2014 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */
package com.couchbase.client.core;

import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import rx.Observable;

/**
 * Represents a Couchbase Cluster.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public interface ClusterFacade {

    /**
     * Sends a {@link CouchbaseRequest} into the cluster and eventually returns a {@link CouchbaseResponse}.
     *
     * The {@link CouchbaseResponse} is not returned directly, but is wrapped into a {@link Observable}.
     *
     * @param request the request to send.
     * @return the {@link CouchbaseResponse} wrapped into a {@link Observable}.
     */
    @InterfaceStability.Committed
    @InterfaceAudience.Public
    @Deprecated
    <R extends CouchbaseResponse> Observable<R> send(CouchbaseRequest request);

    @InterfaceStability.Committed
    @InterfaceAudience.Public
    <R extends CouchbaseResponse> Observable<R> send(final RequestFactory requestFactory);
}
