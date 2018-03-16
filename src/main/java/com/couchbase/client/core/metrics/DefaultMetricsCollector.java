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

import com.couchbase.client.core.event.EventBus;
import com.couchbase.client.core.event.metrics.LatencyMetricsEvent;
import org.HdrHistogram.Histogram;
import org.LatencyUtils.LatencyStats;
import rx.Observable;
import rx.Scheduler;
import rx.functions.Action1;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * The default implementation of the metrics collector.
 *
 * TODO:
 *  - aggregate metrics per service and node
 *  - dont reset histograms on get
 *  - add interval
 *  - make sure interval is respected by histogram
 *  - add interval cleaner that cleans out stale items
 *  - optimize histogram for smaller range and less footprint
 *
 *  - add "tree" to event for nice formatting
 *  - add explanation of the stuff that's in there
 *  - add examples
 *
 * @author Michael Nitschinger
 * @since 1.2.0
 */
public class DefaultMetricsCollector implements MetricsCollector {

    private final Map<MetricIdentifier, LatencyStats> latencyMetrics;

    public DefaultMetricsCollector(final EventBus eventBus, final int emitFrequency, final TimeUnit emitUnit,
        final TimeUnit targetUnit, final Scheduler scheduler) {
        latencyMetrics = new ConcurrentHashMap<MetricIdentifier, LatencyStats>();

        Observable
            .interval(emitFrequency, emitUnit, scheduler)
            .subscribe(new Action1<Long>() {
                @Override
                public void call(Long ignored) {
                    if (eventBus.hasSubscribers()) {
                        List<LatencyMetricsEvent.Metric> metrics = new ArrayList<LatencyMetricsEvent.Metric>();
                        for (Map.Entry<MetricIdentifier, LatencyStats> metric : latencyMetrics.entrySet()) {
                            Histogram histogram = metric.getValue().getIntervalHistogram();
                            metrics.add(new LatencyMetricsEvent.Metric(
                                    metric.getKey().toString(),
                                    targetUnit.convert(histogram.getMinValue(), TimeUnit.NANOSECONDS),
                                    targetUnit.convert(histogram.getMaxValue(), TimeUnit.NANOSECONDS),
                                    histogram.getTotalCount(),
                                    targetUnit.convert(histogram.getValueAtPercentile(99.0), TimeUnit.NANOSECONDS),
                                    targetUnit.convert(histogram.getValueAtPercentile(99.9), TimeUnit.NANOSECONDS),
                                    targetUnit.convert(histogram.getValueAtPercentile(99.99), TimeUnit.NANOSECONDS),
                                    targetUnit.convert(histogram.getValueAtPercentile(99.999), TimeUnit.NANOSECONDS)
                            ));
                        }
                        eventBus.publish(new LatencyMetricsEvent(metrics));
                    }
                }
            });
    }

    @Override
    public void recordLatency(final MetricIdentifier identifier, final long latency) {
        LatencyStats metric = latencyMetrics.get(identifier);
        if (metric == null) {
            metric = new LatencyStats();
            latencyMetrics.put(identifier, metric);
        }
        metric.recordLatency(latency);
    }

}
