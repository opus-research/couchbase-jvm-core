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

import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.ResponseHandler;
import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.service.strategies.SelectionStrategy;
import com.lmax.disruptor.RingBuffer;

public abstract class AbstractPoolingService extends AbstractDynamicService {

    private final int maxEndpoints;
    private final RingBuffer<ResponseEvent> responseBuffer;
    private final SelectionStrategy strategy;

    protected AbstractPoolingService(String hostname, String bucket, String password, int port,
        CoreEnvironment env, int minEndpoints, int maxEndpoints, SelectionStrategy strategy,
        RingBuffer<ResponseEvent> responseBuffer, EndpointFactory endpointFactory) {
        super(hostname, bucket, password, port, env, minEndpoints, responseBuffer, endpointFactory);
        this.maxEndpoints = maxEndpoints;
        this.responseBuffer = responseBuffer;
        this.strategy = strategy;
    }

    @Override
    protected void dispatch(final CouchbaseRequest request) {
        if (endpoints().length == maxEndpoints) {
            Endpoint endpoint = strategy.select(request, endpoints());
            if (endpoint == null) {
                responseBuffer.publishEvent(ResponseHandler.RESPONSE_TRANSLATOR, request, request.observable());
            } else {
                endpoint.send(request);
            }
        } else {
            throw new UnsupportedOperationException("Dynamic endpoint scaling is currently not supported.");
        }
    }

}
