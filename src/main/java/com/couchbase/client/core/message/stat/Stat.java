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

package com.couchbase.client.core.message.stat;

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.message.internal.GetNodesRequest;
import com.couchbase.client.core.message.internal.GetNodesResponse;
import com.couchbase.client.core.message.kv.StatRequest;
import com.couchbase.client.core.message.kv.StatResponse;
import com.couchbase.client.core.service.ServiceType;
import rx.Observable;
import rx.functions.Func0;
import rx.functions.Func1;

/**
 * Utility class to handle STAT calls logic.
 *
 * <pre>
 * {@code
 *  Stat.call(core, bucket.name(), "")
 *      .toBlocking()
 *      .forEach(r -> System.out.printf("%s %s %s\n", r.hostname(), r.key(), r.value()));
 * }
 * </pre>
 *
 * @author Sergey Avseyev
 * @since 1.2.1
 */
public class Stat {
    public static Observable<StatResponse> call(final ClusterFacade core, final String bucket, final String key) {
        return sendStatRequests(core, bucket, key);
    }

    private static Observable<StatResponse> sendStatRequests(final ClusterFacade core, final String bucket, final String key) {
        return Observable.defer(new Func0<Observable<StatResponse>>() {
            @Override
            public Observable<StatResponse> call() {
                Observable<Observable<StatResponse>> stats = core
                        .<GetNodesResponse>send(new GetNodesRequest(ServiceType.BINARY, bucket))
                        .map(new Func1<GetNodesResponse, Observable<StatResponse>>() {
                            @Override
                            public Observable<StatResponse> call(GetNodesResponse node) {
                                return core.send(new StatRequest(key, node.hostname(), bucket));
                            }
                        });
                return Observable.mergeDelayError(stats);
            }
        });
    }
}