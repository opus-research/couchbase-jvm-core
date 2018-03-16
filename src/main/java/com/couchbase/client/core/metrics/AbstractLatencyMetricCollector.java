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

import com.couchbase.client.core.event.CouchbaseEvent;
import com.couchbase.client.core.event.EventBus;
import com.couchbase.client.core.event.metric.AbstractLatencyMetricsEvent;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
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
 * The default implementation of the {@link MetricCollector}, which publishes metrics onto a {@link EventBus}.
 *
 * @author Michael Nitschinger
 * @since 1.2.0
 */
public abstract class AbstractLatencyMetricCollector implements LatencyMetricCollector {

    /**
     * The logger used.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(MetricCollector.class);

    private final MetricCollectorConfig config;
    private final Map<LatencyMetricIdentifier, LatencyStats> latencyMetrics;

    protected abstract CouchbaseEvent generateEvent(List<AbstractLatencyMetricsEvent.Metric> metrics);

    public AbstractLatencyMetricCollector(final EventBus eventBus, final Scheduler scheduler,
        final MetricCollectorConfig config) {
        this.config = config;
        latencyMetrics = new ConcurrentHashMap<LatencyMetricIdentifier, LatencyStats>();

        Observable
            .interval(config.emitFrequency(), config.emitFrequencyUnit(), scheduler)
            .subscribe(new Action1<Long>() {
                @Override
                public void call(Long ignored) {
                    try {
                        if (eventBus.hasSubscribers()) {
                            eventBus.publish(prepareEvent());
                        }
                    } catch (Exception ex) {
                        LOGGER.warn("Caught exception while publishing to event bus.", ex);
                    }
                }
            });
    }

    @Override
    public void recordLatency(final LatencyMetricIdentifier identifier, final long latency) {
        LatencyStats metric = latencyMetrics.get(identifier);
        if (metric == null) {
            metric = new LatencyStats();
            latencyMetrics.put(identifier, metric);
        }
        metric.recordLatency(latency);
    }

    @Override
    public MetricCollectorConfig config() {
        return config;
    }

    /**
     * Helper method to forge an event bus event out of the collected raw metrics.
     *
     * @return the created event, ready to be emitted.
     */
    private CouchbaseEvent prepareEvent() {
        TimeUnit targetUnit = config.targetUnit();

        List<AbstractLatencyMetricsEvent.Metric> metrics = new ArrayList<AbstractLatencyMetricsEvent.Metric>();
        for (Map.Entry<LatencyMetricIdentifier, LatencyStats> metric : latencyMetrics.entrySet()) {
            Histogram histogram = metric.getValue().getIntervalHistogram();

            if (histogram.getTotalCount() == 0) {
                latencyMetrics.remove(metric.getKey());
                continue;
            }

            metrics.add(new AbstractLatencyMetricsEvent.Metric(
                    metric.getKey(),
                    targetUnit.convert(histogram.getMinValue(), TimeUnit.NANOSECONDS),
                    targetUnit.convert(histogram.getMaxValue(), TimeUnit.NANOSECONDS),
                    histogram.getTotalCount(),
                    targetUnit.convert(histogram.getValueAtPercentile(50.0), TimeUnit.NANOSECONDS),
                    targetUnit.convert(histogram.getValueAtPercentile(90.0), TimeUnit.NANOSECONDS),
                    targetUnit.convert(histogram.getValueAtPercentile(95.0), TimeUnit.NANOSECONDS),
                    targetUnit.convert(histogram.getValueAtPercentile(99.0), TimeUnit.NANOSECONDS),
                    targetUnit.convert(histogram.getValueAtPercentile(99.9), TimeUnit.NANOSECONDS)
            ));
        }
        return generateEvent(metrics);
    }
}
