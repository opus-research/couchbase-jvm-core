/**
 * Copyright (c) 2015 Couchbase, Inc.
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
package com.couchbase.client.core.service;

import com.couchbase.client.core.RetryPolicy;
import com.couchbase.client.core.RequestCancelledException;
import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.ResponseHandler;
import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.service.strategies.SelectionStrategy;
import com.lmax.disruptor.RingBuffer;

/**
 * Abstract implementation of a (fixed size) pooling Service.
 *
 * @author Michael Nitschinger
 * @since 1.1.0
 */
public abstract class AbstractPoolingService extends AbstractDynamicService {

    private final int maxEndpoints;
    private final RingBuffer<ResponseEvent> responseBuffer;
    private final SelectionStrategy strategy;
    private final CoreEnvironment env;

    protected AbstractPoolingService(String hostname, String bucket, String password, int port,
        CoreEnvironment env, int minEndpoints, int maxEndpoints, SelectionStrategy strategy,
        RingBuffer<ResponseEvent> responseBuffer, EndpointFactory endpointFactory) {
        super(hostname, bucket, password, port, env, minEndpoints, responseBuffer, endpointFactory);
        this.maxEndpoints = maxEndpoints;
        this.responseBuffer = responseBuffer;
        this.strategy = strategy;
        this.env = env;
    }

    @Override
    protected void dispatch(final CouchbaseRequest request) {
        if (endpoints().length == maxEndpoints) {
            Endpoint endpoint = strategy.select(request, endpoints());
            if (endpoint == null) {
                retryOrCancel(request);
            } else {
                endpoint.send(request);
            }
        } else {
            throw new UnsupportedOperationException("Dynamic endpoint scaling is currently not supported.");
        }
    }

    /**
     * Depending on the policy set, either retry the operation or cancel it right away.
     *
     * @param request the request to retry or cancel.
     */
    private void retryOrCancel(final CouchbaseRequest request) {
        if (env.retryPolicy() == RetryPolicy.BEST_EFFORT) {
            responseBuffer.publishEvent(ResponseHandler.RESPONSE_TRANSLATOR, request, request.observable());
        } else {
            request.observable().onError(new RequestCancelledException("Could not dispatch request to a "
                    + "connected node."));
        }
    }

}
