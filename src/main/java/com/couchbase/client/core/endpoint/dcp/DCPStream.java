/*
 * Copyright (c) 2014 Couchbase, Inc.
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

package com.couchbase.client.core.endpoint.dcp;

import com.couchbase.client.core.message.dcp.DCPRequest;
import rx.subjects.ReplaySubject;
import rx.subjects.Subject;

/**
 * Represents stream of incoming DCP messages.
 *
 * @author Sergey Avseyev
 * @since 1.1.0
 */
public class DCPStream {
    private final int id;
    private final String bucket;
    private final Subject<DCPRequest, DCPRequest> subject;
    private final String connectionName;

    /**
     * Creates new {@link DCPStream} instance.
     *
     * @param id             stream identifier
     * @param connectionName name of DCP connection
     * @param bucket         name of the bucket
     * @param size           size of the subject
     */
    public DCPStream(final int id, final String connectionName, final String bucket, final int size) {
        this.id = id;
        this.bucket = bucket;
        this.connectionName = connectionName;
        subject = ReplaySubject.<DCPRequest>createWithSize(size).toSerialized();
    }

    public Subject<DCPRequest, DCPRequest> subject() {
        return subject;
    }

    public String bucket() {
        return bucket;
    }

    public int id() {
        return id;
    }

    public String connectionName() {
        return connectionName;
    }
}
