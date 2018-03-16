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
package com.couchbase.client.core.service.strategies;

import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.message.CouchbaseRequest;

import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Selects the {@link Endpoint} based on a random selection of connected {@link Endpoint}s.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class RandomSelectionStrategy extends AbstractSelectionStrategy {

    /**
     * Random number generator, statically initialized and designed to be reused.
     */
    private static final Random RANDOM = new Random();

    @Override
    public Endpoint selectOne(final CouchbaseRequest request, final Endpoint[] endpoints) {
        List<Endpoint> selected = selectAllConnected(endpoints);
        if (selected.size() > 0) {
            Collections.shuffle(selected, RANDOM);
            return selected.get(0);
        } else {
            return null;
        }
    }

    @Override
    public Endpoint[] selectAll(CouchbaseRequest request, Endpoint[] endpoints) {
        List<Endpoint> endpointList = selectAllConnected(endpoints);
        return endpointList.toArray(new Endpoint[endpointList.size()]);
    }
}
