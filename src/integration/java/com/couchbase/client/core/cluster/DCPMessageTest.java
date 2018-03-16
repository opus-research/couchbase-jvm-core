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
package com.couchbase.client.core.cluster;

import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.dcp.*;
import com.couchbase.client.core.util.ClusterDependentTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Verifies basic functionality of DCP operations.
 *
 * @author Sergey Avseyev
 * @since 1.0
 */
public class DCPMessageTest extends ClusterDependentTest {

    @Test
    public void shouldOpenConnection() throws Exception {
        OpenConnectionResponse single = cluster()
                .<OpenConnectionResponse>send(new OpenConnectionRequest("hello", "default"))
                .toBlocking()
                .single();
        assertEquals(ResponseStatus.SUCCESS, single.status());
    }

    @Test
    public void shouldAddStream() throws Exception {
        OpenConnectionResponse open = cluster()
                .<OpenConnectionResponse>send(new OpenConnectionRequest("hello", "default"))
                .toBlocking()
                .single();
        assertEquals(ResponseStatus.SUCCESS, open.status());

        AddStreamResponse addStream = cluster()
                .<AddStreamResponse>send(new AddStreamRequest((short) 1, "default"))
                .toBlocking()
                .single();
        assertEquals(ResponseStatus.SUCCESS, addStream.status());
        assertNotEquals(0, addStream.streamIdentifier());
    }

    @Test
    public void shouldRequestStream() throws Exception {
        OpenConnectionResponse open = cluster()
                .<OpenConnectionResponse>send(new OpenConnectionRequest("hello", "default"))
                .toBlocking()
                .single();
        assertEquals(ResponseStatus.SUCCESS, open.status());

        StreamRequestResponse addStream = cluster()
                .<StreamRequestResponse>send(new StreamRequestRequest(0, 0, 0, 0, 0, "default"))
                .toBlocking()
                .single();
        assertEquals(ResponseStatus.SUCCESS, addStream.status());
    }
}
