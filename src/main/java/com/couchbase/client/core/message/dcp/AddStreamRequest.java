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

package com.couchbase.client.core.message.dcp;

/**
 * @author Sergey Avseyev
 * @since 1.0.2
 */
public class AddStreamRequest extends AbstractDCPRequest {
    /**
     *  Specifies that the stream should send over all remaining data to the remote node and then set the remote
     *  nodes vbucket to active state and the source nodes vbucket to dead.
     */
    private final boolean takeOver;
    /**
     *  Specifies that the stream should only send items only if they are on disk. The first item sent is specified
     *  by the start sequence number and items will be sent up to the sequence number specified by the end sequence
     *  number or the last on disk item when the stream is created.
     */
    private final boolean diskOnly;

    public AddStreamRequest(short partition, String bucket) {
        this(partition, false, false, bucket, null);
    }

    public AddStreamRequest(short partition, String bucket, String password) {
        this(partition, false, false, bucket, password);
    }

    public AddStreamRequest(short partition, boolean takeOver, boolean diskOnly, String bucket, String password) {
        super(bucket, password);
        this.takeOver = takeOver;
        this.diskOnly = diskOnly;
        this.partition(partition);
    }

    public boolean diskOnly() {
        return diskOnly;
    }

    public boolean takeOver() {
        return takeOver;
    }
}
