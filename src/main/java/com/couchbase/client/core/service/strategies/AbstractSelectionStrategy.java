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

package com.couchbase.client.core.service.strategies;

import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.state.LifecycleState;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Michael Nitschinger
 * @author Sergey Avseyev
 * @since 1.1.0
 */
public abstract class AbstractSelectionStrategy implements SelectionStrategy {
    /**
     * Helper method to select the first connected endpoint if no particular pinning is needed.
     *
     * @param endpoints the list of endpoints.
     * @return the first connected or null if none found.
     */
    protected static Endpoint selectFirstConnected(final Endpoint[] endpoints) {
        for (Endpoint endpoint : endpoints) {
            if (endpoint.isState(LifecycleState.CONNECTED)) {
                return endpoint;
            }
        }
        return null;
    }

    /**
     * Helper method to select all connected endpoints.
     *
     * @param endpoints the list of endpoints.
     * @return the all connected endpoints.
     */
    protected static List<Endpoint> selectAllConnected(Endpoint[] endpoints) {
        List<Endpoint> selected = new ArrayList<Endpoint>();
        for (Endpoint endpoint : endpoints) {
            if (endpoint.isState(LifecycleState.CONNECTED)) {
                selected.add(endpoint);
            }
        }
        return selected;
    }
}
