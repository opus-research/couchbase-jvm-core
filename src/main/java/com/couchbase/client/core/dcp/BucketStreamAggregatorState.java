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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * State of the stream aggregator.
 *
 * It basically contains list of the stream states.
 *
 * @author Sergey Avseyev
 */
public class BucketStreamAggregatorState implements Iterable<BucketStreamState> {
    /**
     * Default state, which matches all changes in the stream.
     */
    public static final BucketStreamAggregatorState BLANK = new BucketStreamAggregatorState(0);

    private final List<BucketStreamState> feeds;

    /**
     * Creates a new {@link BucketStreamAggregatorState}.
     *
     * @param feeds list containing state of each vBucket
     */
    public BucketStreamAggregatorState(final List<BucketStreamState> feeds) {
        this.feeds = feeds;
    }

    /**
     * Creates a new {@link BucketStreamAggregatorState}.
     * Initializes each entry with empty state BucketStreamState.BLANK.
     *
     * @param numPartitions total number of states.
     */
    public BucketStreamAggregatorState(int numPartitions) {
        feeds = new ArrayList<BucketStreamState>(numPartitions);
    }

    /**
     * Sets state for particular vBucket.
     *
     * @param partition vBucketID (partition number)
     * @param state stream state
     */
    public void feedState(int partition, final BucketStreamState state) {
        feeds.set(partition, state);
    }

    @Override
    public Iterator<BucketStreamState> iterator() {
        return feeds.iterator();
    }

    /**
     * Returns state for the vBucket
     *
     * @param partition vBucketID (partition number)
     * @return state or BucketStreamState.BLANK
     */
    public BucketStreamState get(int partition) {
        if (feeds.size() > partition) {
            return feeds.get(partition);
        } else {
            return BucketStreamState.BLANK;
        }
    }
}
