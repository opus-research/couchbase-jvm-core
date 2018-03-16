package com.couchbase.client.core.endpoint;

import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.ResponseHandler;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;
import com.lmax.disruptor.RingBuffer;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

public abstract class AbstractGenericHandler<RESPONSE, ENCODED, REQUEST extends CouchbaseRequest> extends MessageToMessageCodec<RESPONSE, REQUEST> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractGenericHandler.class);

    private final RingBuffer<ResponseEvent> responseBuffer;
    private final AbstractEndpoint endpoint;
    private final Queue<REQUEST> sentRequestQueue;

    private REQUEST currentRequest;

    protected AbstractGenericHandler(final AbstractEndpoint endpoint, final RingBuffer<ResponseEvent> responseBuffer) {
        this(endpoint, responseBuffer, new ArrayDeque<REQUEST>());
    }

    protected AbstractGenericHandler(final AbstractEndpoint endpoint, final RingBuffer<ResponseEvent> responseBuffer,
        final Queue<REQUEST> queue) {
        this.endpoint = endpoint;
        this.responseBuffer = responseBuffer;
        this.sentRequestQueue = queue;
    }

    protected abstract ENCODED encodeRequest(ChannelHandlerContext ctx, REQUEST msg) throws Exception;
    protected abstract CouchbaseResponse decodeResponse(ChannelHandlerContext ctx, RESPONSE msg) throws Exception;

    @Override
    protected void encode(ChannelHandlerContext ctx, REQUEST msg, List<Object> out) throws Exception {
        ENCODED request = encodeRequest(ctx, msg);
        sentRequestQueue.offer(msg);
        out.add(request);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, RESPONSE msg, List<Object> out) throws Exception {
        if (currentRequest == null) {
            currentRequest = sentRequestQueue.poll();
        }

        CouchbaseResponse response = decodeResponse(ctx, msg);
        responseBuffer.publishEvent(ResponseHandler.RESPONSE_TRANSLATOR, response, currentRequest.observable());
        if (response.status() != ResponseStatus.CHUNKED) {
            currentRequest = null;
        }
    }

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
        LOGGER.debug(logIdent(ctx, endpoint) + "Channel Inactive.");
        endpoint.notifyChannelInactive();
        ctx.fireChannelInactive();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        LOGGER.debug(logIdent(ctx, endpoint) + "Channel Active.");
        ctx.fireChannelActive();
    }

    @Override
    public void channelWritabilityChanged(final ChannelHandlerContext ctx) throws Exception {
        if (!ctx.channel().isWritable()) {
            ctx.flush();
        }
        ctx.fireChannelWritabilityChanged();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (cause instanceof IOException) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(logIdent(ctx, endpoint) + "Connection reset by peer: " + cause.getMessage(), cause);
            } else {
                LOGGER.info(logIdent(ctx, endpoint) + "Connection reset by peer: " + cause.getMessage());
            }
            rescheduleOutstandingOps(ctx);
        } else {
            LOGGER.warn(logIdent(ctx, endpoint) + "Caught unknown exception: " + cause.getMessage(), cause);
            ctx.fireExceptionCaught(cause);
        }
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        rescheduleOutstandingOps(ctx);
    }

    private void rescheduleOutstandingOps(final ChannelHandlerContext ctx) {
        if (sentRequestQueue.isEmpty()) {
            LOGGER.trace(logIdent(ctx, endpoint) + "Not rescheduling any operations - sent request queue empty.");
            return;
        }

        LOGGER.debug(logIdent(ctx, endpoint) + "Rescheduling " + sentRequestQueue.size() + " outstanding requests.");
        while(!sentRequestQueue.isEmpty()) {
            REQUEST req = sentRequestQueue.poll();
            responseBuffer.publishEvent(ResponseHandler.RESPONSE_TRANSLATOR, req, req.observable());
        }
    }

    protected REQUEST currentRequest() {
        return currentRequest;
    }

    protected static String logIdent(final ChannelHandlerContext ctx, final Endpoint endpoint) {
        return "["+ctx.channel().remoteAddress()+"][" + endpoint.getClass().getSimpleName()+"]: ";
    }

}
