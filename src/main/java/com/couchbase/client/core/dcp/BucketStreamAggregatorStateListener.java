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

/**
 * @author Sergey Avseyev
 */
public interface BucketStreamAggregatorStateListener {
    /**
     * Serialize whole state of bucket stream aggregator.
     * Used during substitution of whole state in {@link BucketStreamAggregatorState#replace(BucketStreamState[])}
     *
     * @param aggregatorState the state being serialized
     */
    void onDump(BucketStreamAggregatorState aggregatorState);

    /**
     * Serialize state of updated partition.
     * Used during substitution of partition state in {@link BucketStreamAggregatorState#set(int, BucketStreamState)}
     *
     * @param aggregatorState the container of the state
     * @param partition       updated partition index
     * @param streamState     new state of the stream
     */
    void onDump(BucketStreamAggregatorState aggregatorState, int partition, BucketStreamState streamState);
}
