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

import java.util.concurrent.TimeUnit;

/**
 * The configuration for the metric collector.
 *
 * @author Michael Nitschinger
 * @since 1.2.0
 */
public class DefaultMetricCollectorConfig implements MetricCollectorConfig {

    public static final int DEFAULT_EMIT_FREQUENCY = 30;
    public static final TimeUnit DEFAULT_EMIT_FREQUENCY_UNIT = TimeUnit.SECONDS;
    public static final TimeUnit DEFAULT_TARGET_UNIT = TimeUnit.MICROSECONDS;

    private final int emitFrequency;
    private final TimeUnit emitFrequencyUnit;
    private final TimeUnit targetUnit;

    public static DefaultMetricCollectorConfig create() {
        return new DefaultMetricCollectorConfig(builder());
    }

    public static Builder builder() {
        return new Builder();
    }

    protected DefaultMetricCollectorConfig(final Builder builder) {
        emitFrequency = builder.emitFrequency();
        emitFrequencyUnit = builder.emitFrequencyUnit();
        targetUnit = builder.targetUnit();
    }

    @Override
    public int emitFrequency() {
        return emitFrequency;
    }

    @Override
    public TimeUnit emitFrequencyUnit() {
        return emitFrequencyUnit;
    }

    @Override
    public TimeUnit targetUnit() {
        return targetUnit;
    }

    public static class Builder implements MetricCollectorConfig {

        private int emitFrequency = DEFAULT_EMIT_FREQUENCY;
        private TimeUnit emitFrequencyUnit = DEFAULT_EMIT_FREQUENCY_UNIT;
        private TimeUnit targetUnit = DEFAULT_TARGET_UNIT;

        @Override
        public int emitFrequency() {
            return emitFrequency;
        }

        public Builder emitFrequency(int emitFrequency) {
            this.emitFrequency = emitFrequency;
            return this;
        }

        @Override
        public TimeUnit emitFrequencyUnit() {
            return emitFrequencyUnit;
        }

        public Builder emitFrequencyUnit(final TimeUnit emitFrequencyUnit) {
            this.emitFrequencyUnit = emitFrequencyUnit;
            return this;
        }

        @Override
        public TimeUnit targetUnit() {
            return targetUnit;
        }

        public Builder targetUnit(final TimeUnit targetUnit) {
            this.targetUnit = targetUnit;
            return this;
        }

    }

}
