/*
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
package com.couchbase.client.core.message.kv;

import com.couchbase.client.core.message.CouchbaseResponse;
import rx.subjects.PublishSubject;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author Sergey Avseyev
 * @since 1.2.1
 */
public class StatRequest extends AbstractKeyValueRequest {
    private final InetAddress hostname;
    private final List<StatResponse> responses = Collections.synchronizedList(new ArrayList<StatResponse>());

    public StatRequest(final String key, final InetAddress hostname, final String bucket) {
        super(key, bucket, null, PublishSubject.<CouchbaseResponse>create());
        this.hostname = hostname;
    }

    public void add(final StatResponse response) {
        if (response.key() == null) {
            for (StatResponse statResponse : responses) {
                observable().onNext(statResponse);
            }
            if (!response.status().isSuccess()) {
                observable().onNext(response);
            }
            observable().onCompleted();
        } else {
            responses.add(response);
        }
    }

    public InetAddress hostname() {
        return hostname;
    }

    @Override
    public short partition() {
        return DEFAULT_PARTITION;
    }
}