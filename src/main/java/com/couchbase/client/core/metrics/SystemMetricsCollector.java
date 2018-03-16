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

import com.couchbase.client.core.env.Diagnostics;
import com.couchbase.client.core.event.CouchbaseEvent;
import com.couchbase.client.core.event.EventBus;
import com.couchbase.client.core.event.metrics.SystemMetricsEvent;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.Subscription;
import rx.functions.Action1;

import java.util.Map;
import java.util.TreeMap;

public class SystemMetricsCollector implements MetricsCollector {

    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(SystemMetricsCollector.class);

    private final MetricsCollectorConfig config;
    private final Subscription subscription;

    public SystemMetricsCollector(final EventBus eventBus, Scheduler scheduler, MetricsCollectorConfig config) {
        this.config = config;

        if (config.emitFrequency() > 0) {
            subscription = Observable
                    .interval(config.emitFrequency(), config.emitFrequencyUnit(), scheduler)
                    .subscribe(new Action1<Long>() {
                        @Override
                        public void call(Long ignored) {
                            CouchbaseEvent event = gatherSystemMetrics();
                            if (LOGGER.isTraceEnabled()) {
                                LOGGER.trace("Emitting System Metrics to EventBus: {}", event);
                            }
                            eventBus.publish(event);
                        }
                    });
        } else {
            subscription = null;
            LOGGER.debug("System Metric Emission manually disabled (frequency set to 0).");
        }
    }

    @Override
    public MetricsCollectorConfig config() {
        return config;
    }

    @Override
    public boolean shutdown() {
        if (subscription == null) {
            return true;
        }

        if (!subscription.isUnsubscribed()) {
            subscription.unsubscribe();
        }

        return true;
    }

    private static CouchbaseEvent gatherSystemMetrics() {
        Map<String, Object> metrics = new TreeMap<String, Object>();

        Diagnostics.gcInfo(metrics);
        Diagnostics.memInfo(metrics);
        Diagnostics.threadInfo(metrics);

        return new SystemMetricsEvent(metrics);
    }

}
