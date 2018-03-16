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
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.internal.PlatformDependent;
import rx.subjects.ReplaySubject;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

public class StatRequest extends AbstractKeyValueRequest {
    private static final AtomicIntegerFieldUpdater<StatRequest> endpointCntUpdater;

    static {
        AtomicIntegerFieldUpdater<StatRequest> updater =
                PlatformDependent.newAtomicIntegerFieldUpdater(StatRequest.class, "endpointCnt");
        if (updater == null) {
            updater = AtomicIntegerFieldUpdater.newUpdater(StatRequest.class, "endpointCnt");
        }
        endpointCntUpdater = updater;
    }

    private volatile int endpointCnt = 0;

    public StatRequest(String key, String bucket) {
        super(key, bucket, null, ReplaySubject.<CouchbaseResponse>create(1024).toSerialized());
    }

    public StatRequest start() {
        for (; ; ) {
            int endpointCnt = this.endpointCnt;
            if (endpointCnt < 0) {
                throw new IllegalReferenceCountException(0, 1);
            }
            if (endpointCnt == Integer.MAX_VALUE) {
                throw new IllegalReferenceCountException(Integer.MAX_VALUE, 1);
            }
            if (endpointCntUpdater.compareAndSet(this, endpointCnt, endpointCnt + 1)) {
                break;
            }
        }
        return this;
    }

    public final boolean finish() {
        for (;;) {
            int endpointCnt = this.endpointCnt;
            if (endpointCnt < 0) {
                throw new IllegalReferenceCountException(0, -1);
            }

            if (endpointCntUpdater.compareAndSet(this, endpointCnt, endpointCnt - 1)) {
                return endpointCnt == 1;
            }
        }
    }


    @Override
    public short partition() {
        return DEFAULT_PARTITION;
    }
}
