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
public class OpenConnectionRequest extends AbstractDCPRequest {
    private final ConnectionType type;
    private final int sequenceNumber;
    private final String connectionName;

    public OpenConnectionRequest(String connectionName, String bucket) {
        this(connectionName, ConnectionType.PRODUCER, 0, bucket, null);
    }

    public OpenConnectionRequest(String connectionName, String bucket, String password) {
        this(connectionName, ConnectionType.PRODUCER, 0, bucket, password);
    }

    public OpenConnectionRequest(String connectionName, ConnectionType type, String bucket) {
        this(connectionName, type, 0, bucket, null);
    }

    public OpenConnectionRequest(String connectionName, ConnectionType type, String bucket, String password) {
        this(connectionName, type, 0, bucket, password);
    }

    public OpenConnectionRequest(String connectionName, ConnectionType type, int sequenceNumber, String bucket, String password) {
        super(bucket, password);
        this.type = type;
        this.sequenceNumber = sequenceNumber;
        this.connectionName = connectionName;
    }

    public int sequenceNumber() {
        return sequenceNumber;
    }

    public String connectionName() {
        return connectionName;
    }

    public ConnectionType type() {
        return type;
    }

}
