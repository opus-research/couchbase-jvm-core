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
package com.couchbase.client.core.event.metric;

import com.couchbase.client.core.event.CouchbaseEvent;
import com.couchbase.client.core.event.EventType;

import java.util.List;

/**
 * .
 *
 * @author Michael Nitschinger
 */
public class LatencyMetricsEvent implements CouchbaseEvent {

    List<Metric> metrics;

    public LatencyMetricsEvent(List<Metric> metrics) {
        this.metrics = metrics;
    }

    public List<Metric> metrics() {
        return metrics;
    }

    @Override
    public EventType type() {
        return EventType.METRIC;
    }

    public static class Metric {

        private final String identifier;
        private final long min;
        private final long max;
        private final long count;
        private final double p99;
        private final double p999;
        private final double p9999;

        public Metric(String identifier, long min, long max, long count, double p99, double p999, double p9999) {
            this.identifier = identifier;
            this.min = min;
            this.max = max;
            this.count = count;
            this.p99 = p99;
            this.p999 = p999;
            this.p9999 = p9999;
        }

        public String identifier() {
            return identifier;
        }

        public long min() {
            return min;
        }

        public long max() {
            return max;
        }

        public long count() {
            return count;
        }

        public double p99() {
            return p99;
        }

        public double p999() {
            return p999;
        }

        public double p9999() {
            return p9999;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("Metric{");
            sb.append("identifier='").append(identifier).append('\'');
            sb.append(", min=").append(min);
            sb.append(", max=").append(max);
            sb.append(", count=").append(count);
            sb.append(", p99=").append(p99);
            sb.append(", p999=").append(p999);
            sb.append(", p9999=").append(p9999);
            sb.append('}');
            return sb.toString();
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("LatencyMetricsEvent{");
        sb.append("metrics=").append(metrics);
        sb.append('}');
        return sb.toString();
    }


}