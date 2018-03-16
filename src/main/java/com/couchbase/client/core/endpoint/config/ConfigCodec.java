package com.couchbase.client.core.endpoint.config;

import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.config.BucketConfigRequest;
import com.couchbase.client.core.message.config.BucketConfigResponse;
import com.couchbase.client.core.message.config.ConfigRequest;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageCodec;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.CharsetUtil;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

public class ConfigCodec extends MessageToMessageCodec<FullHttpResponse, ConfigRequest>  {

    /**
     * The Queue which holds the request types so that proper decoding can happen async.
     */
    private final Queue<Class<?>> queue;

    /**
     * Creates a new {@link ConfigCodec} with the default dequeue.
     */
    public ConfigCodec() {
        this(new ArrayDeque<Class<?>>());
    }

    /**
     * Creates a new {@link ConfigCodec} with a custom dequeue.
     *
     * @param queue a custom queue to test encoding/decoding.
     */
    public ConfigCodec(final Queue<Class<?>> queue) {
        this.queue = queue;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, ConfigRequest msg, List<Object> out) throws Exception {
        HttpRequest request;
        if (msg instanceof BucketConfigRequest) {
            request = handleBucketConfigRequest(ctx, (BucketConfigRequest) msg);
        } else {
            throw new IllegalArgumentException("Unknown Message to encode: " + msg);
        }
        out.add(request);
        queue.offer(msg.getClass());
    }

    private HttpRequest handleBucketConfigRequest(ChannelHandlerContext ctx, BucketConfigRequest msg) {
        return new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, msg.path());
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, FullHttpResponse msg, List<Object> out) throws Exception {
        Class<?> request = queue.poll();
        if (request.equals(BucketConfigRequest.class)) {
            out.add(new BucketConfigResponse(msg.content().toString(CharsetUtil.UTF_8), ResponseStatus.SUCCESS));
        }
    }
}
