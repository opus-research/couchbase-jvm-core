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

import com.couchbase.client.core.message.CouchbaseMessage;
import com.couchbase.client.core.message.CouchbaseResponse;
import rx.subjects.Subject;

/**
 * A pre allocated event which carries a {@link CouchbaseResponse} and associated information.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class ResponseEvent {

    /**
     * Contains the current response message.
     */
    private CouchbaseMessage message;

    private Subject<CouchbaseResponse, CouchbaseResponse> observable;

    /**
     * Set the new response as a payload for this event.
     *
     * @param message the response to override.
     * @return the {@link ResponseEvent} for method chaining.
     */
    public ResponseEvent setMessage(final CouchbaseMessage message) {
        this.message = message;
        return this;
    }

    /**
     * Get the response from the payload.
     *
     * @return the actual response.
     */
    public CouchbaseMessage getMessage() {
        return message;
    }

    public Subject<CouchbaseResponse, CouchbaseResponse> getObservable() {
        return observable;
    }

    public ResponseEvent setObservable(final Subject<CouchbaseResponse, CouchbaseResponse> observable) {
        this.observable = observable;
        return this;
    }
}
