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

package com.couchbase.client.core.endpoint.dcp;

import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.endpoint.AbstractEndpoint;
import com.couchbase.client.core.endpoint.AbstractGenericHandler;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.dcp.*;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.BinaryMemcacheRequest;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.BinaryMemcacheResponseStatus;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.DefaultBinaryMemcacheRequest;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.FullBinaryMemcacheResponse;
import com.lmax.disruptor.EventSink;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

import java.util.Queue;

/**
 * @author Sergey Avseyev
 * @since 1.0.2
 */
public class DCPHandler extends AbstractGenericHandler<FullBinaryMemcacheResponse, BinaryMemcacheRequest, DCPRequest> {
    public static final byte OP_OPEN_CONNECTION = (byte) 0x50;
    public static final byte OP_ADD_STREAM = (byte) 0x51;
    public static final byte OP_STREAM_REQUEST = (byte) 0x53;

    public DCPHandler(AbstractEndpoint endpoint, EventSink<ResponseEvent> responseBuffer) {
        super(endpoint, responseBuffer);
    }

    public DCPHandler(AbstractEndpoint endpoint, EventSink<ResponseEvent> responseBuffer, Queue<DCPRequest> queue) {
        super(endpoint, responseBuffer, queue);
    }

    private static ResponseStatus convertStatus(final short status) {
        switch (status) {
            case BinaryMemcacheResponseStatus.SUCCESS:
                return ResponseStatus.SUCCESS;
            default:
                return ResponseStatus.FAILURE;
        }
    }

    @Override
    protected BinaryMemcacheRequest encodeRequest(ChannelHandlerContext ctx, DCPRequest msg) throws Exception {
        BinaryMemcacheRequest request;

        if (msg instanceof OpenConnectionRequest) {
            request = handleOpenConnectionRequest(ctx, (OpenConnectionRequest) msg);
        } else if (msg instanceof AddStreamRequest) {
            request = handleAddStreamRequest(ctx, (AddStreamRequest) msg);
        } else if (msg instanceof StreamRequestRequest) {
            request = handleStreamRequestRequest(ctx, (StreamRequestRequest)msg);
        } else {
            throw new IllegalArgumentException("Unknown incoming DCPRequest type "
                    + msg.getClass());
        }
        if (msg.partition() >= 0) {
            request.setReserved(msg.partition());
        }

        return request;
    }

    @Override
    protected CouchbaseResponse decodeResponse(ChannelHandlerContext ctx, FullBinaryMemcacheResponse msg) throws Exception {
        DCPRequest request = currentRequest();
        ResponseStatus status = convertStatus(msg.getStatus());

        CouchbaseResponse response;
        ByteBuf content = msg.content().copy();

        if (request instanceof OpenConnectionRequest) {
            response = new OpenConnectionResponse(status, request);
        } else if (request instanceof AddStreamRequest) {
            int streamIdentifier = content.getInt(0);
            response = new AddStreamResponse(status, streamIdentifier, request);
        } else if (request instanceof StreamRequestRequest) {
            response = new StreamRequestResponse(status, request);
        } else {
            throw new IllegalStateException("Unhandled request/response pair: " + request.getClass() + "/"
                    + msg.getClass());
        }

        finishedDecoding();
        return response;
    }

    private BinaryMemcacheRequest handleOpenConnectionRequest(ChannelHandlerContext ctx, OpenConnectionRequest msg) {
        ByteBuf extras = ctx.alloc().buffer(8);
        extras.writeInt(msg.sequenceNumber());
        extras.writeInt(msg.type().flags());
        byte extrasLength = (byte) extras.readableBytes();

        String key = msg.connectionName();
        short keyLength = (short) key.length();
        BinaryMemcacheRequest request = new DefaultBinaryMemcacheRequest(key, extras);
        request.setOpcode(OP_OPEN_CONNECTION);
        request.setKeyLength(keyLength);
        request.setExtrasLength(extrasLength);
        request.setTotalBodyLength(keyLength + extrasLength);

        return request;
    }

    private BinaryMemcacheRequest handleAddStreamRequest(ChannelHandlerContext ctx, AddStreamRequest msg) {
        int flags = 0;
        if (msg.takeOver()) {
            flags |= 0x01;
        }
        if (msg.diskOnly()) {
            flags |= 0x02;
        }
        ByteBuf extras = ctx.alloc().buffer(8);
        extras.writeInt(flags);
        byte extrasLength = (byte) extras.readableBytes();

        BinaryMemcacheRequest request = new DefaultBinaryMemcacheRequest(extras);
        request.setOpcode(OP_ADD_STREAM);
        request.setExtrasLength(extrasLength);
        request.setTotalBodyLength(extrasLength);

        return request;
    }

    private BinaryMemcacheRequest handleStreamRequestRequest(ChannelHandlerContext ctx, StreamRequestRequest msg) {
        ByteBuf extras = ctx.alloc().buffer(48);
        extras.writeInt(0).writeInt(0);
        extras.writeLong(msg.startSequenceNumber());
        extras.writeLong(msg.endSequenceNumber());
        extras.writeLong(msg.vbucketUUID());
        extras.writeLong(msg.snapshotStartSequenceNumber());
        extras.writeLong(msg.snapshotEndSequenceNumber());
        byte extrasLength = (byte) extras.readableBytes();

        BinaryMemcacheRequest request = new DefaultBinaryMemcacheRequest(extras);
        request.setOpcode(OP_STREAM_REQUEST);
        request.setExtrasLength(extrasLength);
        request.setTotalBodyLength(extrasLength);

        return request;
    }
}
