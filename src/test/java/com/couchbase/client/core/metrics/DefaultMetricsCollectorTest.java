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
package com.couchbase.client.core.metrics;


import com.couchbase.client.core.event.CouchbaseEvent;
import com.couchbase.client.core.event.DefaultEventBus;
import com.couchbase.client.core.event.EventBus;
import com.couchbase.client.core.service.ServiceType;
import org.junit.Test;
import rx.observers.TestSubscriber;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class DefaultMetricsCollectorTest {

    @Test
    public void shouldFeedLatencyIntoEventBus() throws Exception {
        TestScheduler scheduler = Schedulers.test();
        EventBus eventBus = new DefaultEventBus(scheduler);

        TestSubscriber<CouchbaseEvent> testSubscriber = new TestSubscriber<CouchbaseEvent>();
        eventBus.get().subscribe(testSubscriber);

        MetricsCollector collector = new DefaultMetricsCollector(eventBus, 5, TimeUnit.SECONDS, TimeUnit.MICROSECONDS, scheduler);
        assertEquals(0, testSubscriber.getOnNextEvents().size());

        collector.recordLatency(new MetricIdentifier("127.0.0.1", ServiceType.BINARY, "get"), 500);
        collector.recordLatency(new MetricIdentifier("127.0.0.1", ServiceType.BINARY, "get"), 600);
        collector.recordLatency(new MetricIdentifier("127.0.0.1", ServiceType.BINARY, "set"), 200);

        collector.recordLatency(new MetricIdentifier("127.0.0.1", ServiceType.QUERY, "get"), 1200);

        scheduler.advanceTimeBy(6, TimeUnit.SECONDS);
        assertEquals(1, testSubscriber.getOnNextEvents().size());
    }
}