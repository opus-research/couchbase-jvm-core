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

package com.couchbase.client.core.dcp;

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.message.cluster.GetClusterConfigRequest;
import com.couchbase.client.core.message.cluster.GetClusterConfigResponse;
import com.couchbase.client.core.message.dcp.DCPRequest;
import com.couchbase.client.core.message.dcp.OpenConnectionRequest;
import com.couchbase.client.core.message.dcp.OpenConnectionResponse;
import com.couchbase.client.core.message.dcp.StreamRequestRequest;
import com.couchbase.client.core.message.dcp.StreamRequestResponse;
import rx.Observable;
import rx.functions.Func1;

/**
 * Provides a higher level abstraction over a DCP stream.
 *
 * The bucket is expected to be opened already when the {@link #feed()} method is called.
 *
 * @author Michael Nitschinger
 * @since 1.1.0
 */
public class BucketStreamAggregator {

    private final ClusterFacade core;
    private final String bucket;

    public BucketStreamAggregator(final ClusterFacade core, final String bucket) {
        this.core = core;
        this.bucket = bucket;
    }

    /**
     * Opens a DCP stream and returns the feed of changes.
     *
     * Only open it once for now.
     * @return the feed with {@link DCPRequest}s.
     */
    public Observable<DCPRequest> feed() {
        return core
            .<OpenConnectionResponse>send(new OpenConnectionRequest("dcpStream", bucket))
            .flatMap(new Func1<OpenConnectionResponse, Observable<Integer>>() {
                @Override
                public Observable<Integer> call(OpenConnectionResponse openConnectionResponse) {
                    return partitionSize();
                }
            })
            .flatMap(new Func1<Integer, Observable<DCPRequest>>() {
                @Override
                public Observable<DCPRequest> call(Integer numPartitions) {
                    return Observable.merge(
                        Observable
                            .range(0, numPartitions)
                            .flatMap(new Func1<Integer, Observable<StreamRequestResponse>>() {
                                @Override
                                public Observable<StreamRequestResponse> call(Integer partition) {
                                    return core.send(new StreamRequestRequest(partition.shortValue(), bucket));
                                }
                            })
                            .map(new Func1<StreamRequestResponse, Observable<DCPRequest>>() {
                                @Override
                                public Observable<DCPRequest> call(StreamRequestResponse response) {
                                    return response.stream();
                                }
                            })
                    );
                }
            });
    }

    /**
     * Helper method to fetch the number of partitions.
     *
     * @return the number of partitions.
     */
    private Observable<Integer> partitionSize() {
        return core
            .<GetClusterConfigResponse>send(new GetClusterConfigRequest())
            .map(new Func1<GetClusterConfigResponse, Integer>() {
                @Override
                public Integer call(GetClusterConfigResponse response) {
                    CouchbaseBucketConfig config = (CouchbaseBucketConfig) response.config().bucketConfig(bucket);
                    return config.numberOfPartitions();
                }
            });
    }
}
