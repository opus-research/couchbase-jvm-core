/**
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
package com.couchbase.client.core.metrics;

import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.service.ServiceType;

/**
 * Identifies a metric throughout the collector.
 *
 * @author Michael Nitschinger
 * @since 1.2.0
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public class LatencyMetricIdentifier implements Comparable<LatencyMetricIdentifier> {

    private final String node;
    private final ServiceType serviceType;
    private final String suffix;

    public LatencyMetricIdentifier(String node, ServiceType serviceType, String suffix) {
        this.node = node;
        this.serviceType = serviceType;
        this.suffix = suffix;
    }

    @Override
    public String toString() {
        return node + "#" + serviceType + "#" + suffix;
    }

    @Override
    public int compareTo(LatencyMetricIdentifier o) {
        return toString().compareTo(o.toString());
    }

    @Override
    public boolean equals(java.lang.Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LatencyMetricIdentifier that = (LatencyMetricIdentifier) o;

        if (node != null ? !node.equals(that.node) : that.node != null) return false;
        if (serviceType != that.serviceType) return false;
        return !(suffix != null ? !suffix.equals(that.suffix) : that.suffix != null);

    }

    @Override
    public int hashCode() {
        int result = node != null ? node.hashCode() : 0;
        result = 31 * result + (serviceType != null ? serviceType.hashCode() : 0);
        result = 31 * result + (suffix != null ? suffix.hashCode() : 0);
        return result;
    }
}
