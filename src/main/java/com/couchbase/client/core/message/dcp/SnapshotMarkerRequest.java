/*
 * Copyright (c) 2014 Couchbase, Inc.
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

package com.couchbase.client.core.message.dcp;

/**
 * @author Sergey Avseyev
 * @since 1.0.2
 */
public class SnapshotMarkerRequest extends AbstractDCPRequest {
    public static final int MEMORY = 0x01;
    public static final int DISK = 0x02;
    public static final int CHECK = 0x04;
    public static final int ACK = 0x08;

    private final long startSequenceNumber;
    private final long endSequenceNumber;

    private final boolean memory;
    private final boolean disk;
    private final boolean check;
    private final boolean ack;

    public SnapshotMarkerRequest(short partition, long startSequenceNumber, long endSequenceNumber, int flags, String bucket) {
        this(partition, startSequenceNumber, endSequenceNumber, flags, bucket, null);
    }

    public SnapshotMarkerRequest(short partition, long startSequenceNumber, long endSequenceNumber, int flags, String bucket, String password) {
        super(bucket, password);
        partition(partition);
        this.startSequenceNumber = startSequenceNumber;
        this.endSequenceNumber = endSequenceNumber;
        this.memory = (flags & MEMORY) == MEMORY;
        this.disk = (flags & DISK) == DISK;
        this.check = (flags & CHECK) == CHECK;
        this.ack = (flags & ACK) == ACK;
    }
}
