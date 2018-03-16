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

import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.event.CouchbaseEvent;
import com.couchbase.client.core.event.EventType;
import com.couchbase.client.core.metrics.MetricIdentifier;

import java.util.List;

/**
 * An event which contains latency metrics collected from the core.
 *
 * @author Michael Nitschinger
 * @since 1.2.0
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
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

        private final MetricIdentifier identifier;
        private final long min;
        private final long max;
        private final long count;
        private final double p50;
        private final double p90;
        private final double p95;
        private final double p99;
        private final double p999;

        public Metric(MetricIdentifier identifier, long min, long max, long count, double p50, double p90, double p95,
            double p99, double p999) {
            this.identifier = identifier;
            this.min = min;
            this.max = max;
            this.count = count;
            this.p50 = p50;
            this.p90 = p90;
            this.p95 = p95;
            this.p99 = p99;
            this.p999 = p999;
        }

        public MetricIdentifier identifier() {
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

        public double p50() {
            return p50;
        }

        public double p90() {
            return p90;
        }

        public double p95() {
            return p95;
        }

        public double p99() {
            return p99;
        }

        public double p999() {
            return p999;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("Metric{");
            sb.append("identifier='").append(identifier).append('\'');
            sb.append(", min=").append(min);
            sb.append(", max=").append(max);
            sb.append(", count=").append(count);
            sb.append(", p50=").append(p50);
            sb.append(", p90=").append(p90);
            sb.append(", p95=").append(p95);
            sb.append(", p99=").append(p99);
            sb.append(", p999=").append(p999);
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