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

import org.junit.Test;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ExponentialDelayTest {

    @Test
    public void testPowerOfTwoShouldCalculateExponentially() {
        Delay exponentialDelay = new ExponentialDelay(TimeUnit.SECONDS, Integer.MAX_VALUE, 0, 1, 2);

        assertEquals(0, exponentialDelay.calculate(0));
        assertEquals(1, exponentialDelay.calculate(1));
        assertEquals(2, exponentialDelay.calculate(2));
        assertEquals(4, exponentialDelay.calculate(3));
        assertEquals(8, exponentialDelay.calculate(4));
        assertEquals(16, exponentialDelay.calculate(5));
        assertEquals(32, exponentialDelay.calculate(6));

        assertEquals("ExponentialDelay{growBy 1.0 SECONDS, powers of 2; lower=0, upper=2147483647}", exponentialDelay.toString());

    }

    @Test
    public void testPowerOfTwoShouldRespectLowerBound() {
        Delay exponentialDelay = new ExponentialDelay(TimeUnit.SECONDS, Integer.MAX_VALUE, 10, 1, 2);

        assertEquals(10, exponentialDelay.calculate(0));
        assertEquals(10, exponentialDelay.calculate(1));
        assertEquals(10, exponentialDelay.calculate(2));
        assertEquals(10, exponentialDelay.calculate(3));
        assertEquals(10, exponentialDelay.calculate(4));
        assertEquals(16, exponentialDelay.calculate(5));
        assertEquals(32, exponentialDelay.calculate(6));

        assertEquals("ExponentialDelay{growBy 1.0 SECONDS, powers of 2; lower=10, upper=2147483647}", exponentialDelay.toString());
    }

    @Test
    public void testPowerOfTwoShouldRespectUpperBound() {
        Delay exponentialDelay = new ExponentialDelay(TimeUnit.SECONDS, 9, 0, 1, 2);

        assertEquals(0, exponentialDelay.calculate(0));
        assertEquals(1, exponentialDelay.calculate(1));
        assertEquals(2, exponentialDelay.calculate(2));
        assertEquals(4, exponentialDelay.calculate(3));
        assertEquals(8, exponentialDelay.calculate(4));
        assertEquals(9, exponentialDelay.calculate(5));
        assertEquals(9, exponentialDelay.calculate(6));

        assertEquals("ExponentialDelay{growBy 1.0 SECONDS, powers of 2; lower=0, upper=9}", exponentialDelay.toString());
    }

    @Test
    public void testPowerOfTwoShouldApplyFactor() {
        Delay exponentialDelay = new ExponentialDelay(TimeUnit.SECONDS, Integer.MAX_VALUE, 0, 10, 2);

        assertEquals(0, exponentialDelay.calculate(0));
        assertEquals(10, exponentialDelay.calculate(1));
        assertEquals(20, exponentialDelay.calculate(2));
        assertEquals(40, exponentialDelay.calculate(3));
        assertEquals(80, exponentialDelay.calculate(4));
        assertEquals(160, exponentialDelay.calculate(5));
        assertEquals(320, exponentialDelay.calculate(6));

        assertEquals("ExponentialDelay{growBy 10.0 SECONDS, powers of 2; lower=0, upper=2147483647}", exponentialDelay.toString());
    }

    @Test
    public void testPowerOfTwoShouldNotOverflowInAMillionRetries() {
        Delay exponentialDelay = new ExponentialDelay(TimeUnit.SECONDS, Long.MAX_VALUE, 0, 2d, 2);

        long previous = Long.MIN_VALUE;
        //the bitwise operation in ExponentialDelay would overflow the step at i = 32
        for(int i = 0; i < 1000000; i++) {
            long now = exponentialDelay.calculate(i);
            assertTrue("delay is at " + now + " down from " + previous + ", attempt " + i, now >= previous);
            previous = now;
        }
    }

    @Test
    public void testPowerOfTenShouldCalculateExponentially() {
        Delay exponentialDelay = new ExponentialDelay(TimeUnit.SECONDS, Integer.MAX_VALUE, 0, 1, 10);

        assertEquals(0, exponentialDelay.calculate(0));
        assertEquals(1, exponentialDelay.calculate(1)); //1 * 10^0
        assertEquals(10, exponentialDelay.calculate(2)); //1 * 10^1
        assertEquals(100, exponentialDelay.calculate(3)); //1 * 10^2
        assertEquals(1000, exponentialDelay.calculate(4));
        assertEquals(10000, exponentialDelay.calculate(5));
        assertEquals(100000, exponentialDelay.calculate(6));

        assertEquals("ExponentialDelay{growBy 1.0 SECONDS, powers of 10; lower=0, upper=2147483647}", exponentialDelay.toString());
    }

    @Test
    public void testPowerOfTenShouldRespectLowerBound() {
        Delay exponentialDelay = new ExponentialDelay(TimeUnit.SECONDS, Integer.MAX_VALUE, 400, 1, 10);

        assertEquals(400, exponentialDelay.calculate(0));
        assertEquals(400, exponentialDelay.calculate(1));
        assertEquals(400, exponentialDelay.calculate(2));
        assertEquals(400, exponentialDelay.calculate(3));
        assertEquals(1000, exponentialDelay.calculate(4));
        assertEquals(10000, exponentialDelay.calculate(5));
        assertEquals(100000, exponentialDelay.calculate(6));

        assertEquals("ExponentialDelay{growBy 1.0 SECONDS, powers of 10; lower=400, upper=2147483647}", exponentialDelay.toString());
    }

    @Test
    public void testPowerOfTenShouldRespectUpperBound() {
        Delay exponentialDelay = new ExponentialDelay(TimeUnit.SECONDS, 500, 0, 1, 10);

        assertEquals(0, exponentialDelay.calculate(0));
        assertEquals(1, exponentialDelay.calculate(1));
        assertEquals(10, exponentialDelay.calculate(2));
        assertEquals(100, exponentialDelay.calculate(3));
        assertEquals(500, exponentialDelay.calculate(4));
        assertEquals(500, exponentialDelay.calculate(5));
        assertEquals(500, exponentialDelay.calculate(6));

        assertEquals("ExponentialDelay{growBy 1.0 SECONDS, powers of 10; lower=0, upper=500}", exponentialDelay.toString());
    }

    @Test
    public void testPowerOfTenShouldApplyFactor() {
        Delay exponentialDelay = new ExponentialDelay(TimeUnit.SECONDS, Integer.MAX_VALUE, 0, 2, 10);

        assertEquals(0, exponentialDelay.calculate(0));
        assertEquals(2, exponentialDelay.calculate(1));
        assertEquals(20, exponentialDelay.calculate(2));
        assertEquals(200, exponentialDelay.calculate(3));
        assertEquals(2000, exponentialDelay.calculate(4));
        assertEquals(20000, exponentialDelay.calculate(5));
        assertEquals(200000, exponentialDelay.calculate(6));

        assertEquals("ExponentialDelay{growBy 2.0 SECONDS, powers of 10; lower=0, upper=2147483647}", exponentialDelay.toString());
    }

    @Test
    public void testPowerOfTenShouldNotOverflowInAMillionRetries() {
        Delay exponentialDelay = new ExponentialDelay(TimeUnit.SECONDS, Long.MAX_VALUE, 0, 2d, 10);

        long previous = Long.MIN_VALUE;
        for(int i = 0; i < 1000000; i++) {
            long now = exponentialDelay.calculate(i);
            assertTrue("delay is at " + now + " down from " + previous + ", attempt " + i, now >= previous);
            previous = now;
        }
    }

    @Test
    public void testAlternatePowerAndBitshiftPowerProduceSameResult() {
        ExponentialDelay exponentialDelay = new ExponentialDelay(TimeUnit.SECONDS, Long.MAX_VALUE, 0, 1, 2);

        for(int i = 1; i < 1000000; i++) {
            long powValue = exponentialDelay.calculateAlternatePower(i);
            long bitshiftValue = exponentialDelay.calculatePowerOfTwo(i);

            assertEquals("difference at step " + i, powValue, bitshiftValue);
        }
    }

}