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
 * Represents the entry point into the core layer.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
@InterfaceStability.Committed
@InterfaceAudience.Public
public interface ClusterFacade {

    /**
     * Sends a {@link CouchbaseRequest} into the cluster and eventually returns a {@link CouchbaseResponse}.
     *
     * The {@link CouchbaseResponse} is not returned directly, but is wrapped into a cold {@link Observable}.
     *
     * Note that this method is deprecated in favor of {@link #send(RequestFactory)}, because while it makes the
     * Observable cold, it still reuses the same originating hot subject for retry logic, which can result in
     * correctness issues. Instead, the factory send should be used where on every factory call a fresh new
     * request is created.
     *
     * @param request the request to send.
     * @return the {@link CouchbaseResponse} wrapped into a cold {@link Observable}.
     */
    @InterfaceStability.Committed
    @InterfaceAudience.Public
    @Deprecated
    <R extends CouchbaseResponse> Observable<R> send(CouchbaseRequest request);

    /**
     * Sends a {@link CouchbaseRequest} into the cluster and eventually returns a {@link CouchbaseResponse}.
     *
     * The {@link CouchbaseResponse} is not returned directly, but is wrapped into a cold {@link Observable}.
     *
     * Note that the factory implementation needs to make sure that on every call, a fresh new request is created,
     * in other to properly handle retry scenarios. The reason for this is that a hot observable is bound on every
     * request object, so the only way to handle it is to create a new one.
     *
     * @param requestFactory the factory which forges requests on demand.
     * @return the {@link CouchbaseResponse} wrapped into a cold {@link Observable}.
     */
    @InterfaceStability.Committed
    @InterfaceAudience.Public
    <R extends CouchbaseResponse> Observable<R> send(final RequestFactory requestFactory);
}
