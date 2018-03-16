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
package com.couchbase.client.core.time;

import java.util.concurrent.TimeUnit;

/**
 * Parent class of {@link Delay}s and provides factory methods to create them.
 *
 * @author Michael Nitschinger
 * @since 1.1.0
 */
public abstract class Delay {

    /**
     * The time unit of the delay.
     */
    private final TimeUnit unit;

    /**
     * Creates a new {@link Delay}.
     *
     * @param unit the time unit.
     */
    Delay(TimeUnit unit) {
        if (unit == null) {
            throw new IllegalArgumentException("TimeUnit is not allowed to be null");
        }

        this.unit = unit;
    }

    /**
     * Returns the {@link TimeUnit} associated with this {@link Delay}.
     *
     * @return the time unit.
     */
    public TimeUnit unit() {
        return unit;
    }

    /**
     * Calculate a specific delay based on the attempt passed in.
     *
     * This method is to be implemented by the child implementations and depending on the params
     * that were set during construction time.
     *
     * @param attempt the attempt to calculate the delay from.
     * @return the calculate delay.
     */
    public abstract long calculate(long attempt);

    /**
     * Creates a new {@link FixedDelay}.
     *
     * @param time the time of the delay.
     * @param unit the unit of the delay.
     * @return a created {@link FixedDelay}.
     */
    public static Delay fixed(long time, TimeUnit unit) {
        return new FixedDelay(time, unit);
    }

    /**
     * Creates a new {@link LinearDelay} with no bounds and default factor.
     *
     * @param unit the unit of the delay.
     * @return a created {@link LinearDelay}.
     */
    public static Delay linear(TimeUnit unit) {
        return linear(unit, Long.MAX_VALUE);
    }

    /**
     * Creates a new {@link LinearDelay} with a custom upper boundary and the default factor.
     *
     * @param unit the unit of the delay.
     * @param upper the upper boundary.
     * @return a created {@link LinearDelay}.
     */
    public static Delay linear(TimeUnit unit, long upper) {
        return linear(unit, upper, 0);
    }

    /**
     * Creates a new {@link LinearDelay} with a custom boundaries and the default factor.
     *
     * @param unit the unit of the delay.
     * @param upper the upper boundary.
     * @param lower the lower boundary.
     * @return a created {@link LinearDelay}.
     */
    public static Delay linear(TimeUnit unit, long upper, long lower) {
        return linear(unit, upper, lower, 1);
    }

    /**
     * Creates a new {@link LinearDelay} with a custom boundaries and factor.
     *
     * @param unit the unit of the delay.
     * @param upper the upper boundary.
     * @param lower the lower boundary.
     * @param factor the multiplication factor.
     * @return a created {@link LinearDelay}.
     */
    public static Delay linear(TimeUnit unit, long upper, long lower, long factor) {
        return new LinearDelay(unit, upper, lower, factor);
    }

    /**
     * Creates a new {@link ExponentialDelay} with default boundaries and factor.
     *
     * @param unit the unit of the delay.
     * @return a created {@link ExponentialDelay}.
     */
    public static Delay exponential(TimeUnit unit) {
        return exponential(unit, Long.MAX_VALUE);
    }

    /**
     * Creates a new {@link ExponentialDelay} with custom upper boundary and default factor.
     *
     * @param unit the unit of the delay.
     * @param upper the upper boundary.
     * @return a created {@link ExponentialDelay}.
     */
    public static Delay exponential(TimeUnit unit, long upper) {
        return exponential(unit, upper, 0);
    }

    /**
     * Creates a new {@link ExponentialDelay} with custom boundaries and default factor.
     *
     * @param unit the unit of the delay.
     * @param upper the upper boundary.
     * @param lower the lower boundary.
     * @return a created {@link ExponentialDelay}.
     */
    public static Delay exponential(TimeUnit unit, long upper, long lower) {
        return exponential(unit, upper, lower, 1);
    }

    /**
     * Creates a new {@link ExponentialDelay} with custom boundaries and factor.
     *
     * @param unit the unit of the delay.
     * @param upper the upper boundary.
     * @param lower the lower boundary.
     * @param factor the multiplication factor.
     * @return a created {@link ExponentialDelay}.
     */
    public static Delay exponential(TimeUnit unit, long upper, long lower, long factor) {
        return new ExponentialDelay(unit, upper, lower, factor);
    }

}
