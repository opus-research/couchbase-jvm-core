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
import rx.Notification;
import rx.functions.Action1;
import rx.subjects.BehaviorSubject;
import rx.subjects.PublishSubject;
import rx.subjects.ReplaySubject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class StatRequest extends AbstractKeyValueRequest {

    private volatile AtomicLong endpointCnt = new AtomicLong(0);
    private volatile int completed = 0;
    List<StatResponse> responses = Collections.synchronizedList(new ArrayList<StatResponse>());

    public StatRequest(String key, String bucket) {
        super(key, bucket, null, ReplaySubject.<CouchbaseResponse>create().toSerialized());
    }

    public void add(StatResponse response) {
        responses.add(response);
    }

    public List<StatResponse> responses() {
        return responses;
    }


    public void onCompleted() {
        completed ++;
        if (completed == endpointCnt.get()) {
            observable().onCompleted();
            System.out.println("Observable has been completed from StatRequest");

        }
    }

    public StatRequest start() {
        long x = endpointCnt.incrementAndGet();
        System.out.println("INC endpointCnt = " + x);
        return this;
    }

    public final boolean finish() {
        long x = endpointCnt.decrementAndGet();
        System.out.println("DEC endpointCnt = " + x);
        return x == 0;
    }

    @Override
    public short partition() {
        return DEFAULT_PARTITION;
    }
}
