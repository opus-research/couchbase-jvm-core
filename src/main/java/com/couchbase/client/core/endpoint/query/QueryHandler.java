/*
 * Copyright (c) 2016 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.core.endpoint.query;

import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.endpoint.AbstractEndpoint;
import com.couchbase.client.core.endpoint.AbstractGenericHandler;
import com.couchbase.client.core.endpoint.ResponseStatusConverter;
import com.couchbase.client.core.endpoint.query.parser.QueryResponseParser;
import com.couchbase.client.core.endpoint.query.parser.QueryResponseParserV1;
import com.couchbase.client.core.endpoint.query.parser.QueryResponseParserV2;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.AbstractCouchbaseRequest;
import com.couchbase.client.core.message.AbstractCouchbaseResponse;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.KeepAlive;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.query.GenericQueryRequest;
import com.couchbase.client.core.message.query.QueryRequest;
import com.couchbase.client.core.message.query.RawQueryRequest;
import com.couchbase.client.core.message.query.RawQueryResponse;
import com.couchbase.client.core.service.ServiceType;
import com.lmax.disruptor.RingBuffer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import java.util.Queue;

/**
 * The {@link QueryHandler} is responsible for encoding {@link QueryRequest}s into lower level
 * {@link HttpRequest}s as well as decoding {@link HttpObject}s into
 * {@link CouchbaseResponse}s.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class QueryHandler extends AbstractGenericHandler<HttpObject, HttpRequest, QueryRequest> {

    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(QueryHandler.class);

    /**
     * Contains the current pending response header if set.
     */
    private HttpResponse responseHeader;

    /**
     * Contains the accumulating buffer for the response content.
     */
    private ByteBuf responseContent;

    private QueryResponseParser parser;

    /**
     * Creates a new {@link QueryHandler} with the default queue for requests.
     *
     * @param endpoint the {@link AbstractEndpoint} to coordinate with.
     * @param responseBuffer the {@link RingBuffer} to push responses into.
     */
    public QueryHandler(AbstractEndpoint endpoint, RingBuffer<ResponseEvent> responseBuffer, boolean isTransient,
                        final boolean pipeline) {
        super(endpoint, responseBuffer, isTransient, pipeline);
        createParser(false);
    }

    /**
     * Creates a new {@link QueryHandler} with a custom queue for requests (suitable for tests).
     *
     * @param endpoint the {@link AbstractEndpoint} to coordinate with.
     * @param responseBuffer the {@link RingBuffer} to push responses into.
     * @param queue the queue which holds all outstanding open requests.
     */
    QueryHandler(AbstractEndpoint endpoint, RingBuffer<ResponseEvent> responseBuffer, Queue<QueryRequest> queue,
                 boolean isTransient, final boolean pipeline) {
        super(endpoint, responseBuffer, queue, isTransient, pipeline);
        createParser(false);
    }

    /**
     * Creates a new {@link QueryHandler} with a custom queue for requests with parser selection (suitable for tests).
     *
     * @param endpoint the {@link AbstractEndpoint} to coordinate with.
     * @param responseBuffer the {@link RingBuffer} to push responses into.
     * @param queue the queue which holds all outstanding open requests.
     */
    protected QueryHandler(AbstractEndpoint endpoint, RingBuffer<ResponseEvent> responseBuffer, Queue<QueryRequest> queue,
                 boolean isTransient, final boolean pipeline, boolean enableV2) {
        super(endpoint, responseBuffer, queue, isTransient, pipeline);
        createParser(enableV2);
    }

    private void createParser(boolean enableV2) {
        boolean v2EnabledBySystemProperty = System.getProperty("com.couchbase.enableNewQueryResponseParser") != null;
        if (v2EnabledBySystemProperty || enableV2) {
            parser = new QueryResponseParserV2(env().scheduler(), env().autoreleaseAfter());
        } else {
            parser = new QueryResponseParserV1(env().scheduler(), env().autoreleaseAfter());
        }
    }

    @Override
    protected HttpRequest encodeRequest(final ChannelHandlerContext ctx, final QueryRequest msg) throws Exception {
        FullHttpRequest request;

        if (msg instanceof GenericQueryRequest) {
            GenericQueryRequest queryRequest = (GenericQueryRequest) msg;
            request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/query");
            request.headers().set(HttpHeaders.Names.USER_AGENT, env().userAgent());
            if (queryRequest.isJsonFormat()) {
                request.headers().set(HttpHeaders.Names.CONTENT_TYPE, "application/json");
            }
            ByteBuf query = ctx.alloc().buffer(((GenericQueryRequest) msg).query().length());
            query.writeBytes(((GenericQueryRequest) msg).query().getBytes(CHARSET));
            request.headers().add(HttpHeaders.Names.CONTENT_LENGTH, query.readableBytes());
            request.headers().set(HttpHeaders.Names.HOST, remoteHttpHost(ctx));
            request.content().writeBytes(query);
            query.release();
        } else if (msg instanceof KeepAliveRequest) {
            request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/admin/ping");
            request.headers().set(HttpHeaders.Names.USER_AGENT, env().userAgent());
            request.headers().set(HttpHeaders.Names.HOST, remoteHttpHost(ctx));
            return request;
        } else {
            throw new IllegalArgumentException("Unknown incoming QueryRequest type "
                + msg.getClass());
        }

        addHttpBasicAuth(ctx, request, msg.bucket(), msg.password());
        return request;
    }

    @Override
    protected CouchbaseResponse decodeResponse(final ChannelHandlerContext ctx, final HttpObject msg) throws Exception {
        CouchbaseResponse response = null;

        if (msg instanceof HttpResponse) {
            responseHeader = (HttpResponse) msg;
            if (responseContent != null) {
                responseContent.clear();
            } else {
                responseContent = ctx.alloc().buffer();
            }
        }

        if (currentRequest() instanceof KeepAliveRequest) {
            if (msg instanceof LastHttpContent) {
                response = new KeepAliveResponse(ResponseStatusConverter.fromHttp(responseHeader.getStatus().code()), currentRequest());
                responseContent.clear();
                responseContent.discardReadBytes();
                finishedDecoding();
            }
        } else if (msg instanceof HttpContent) {
            responseContent.writeBytes(((HttpContent) msg).content());
            boolean lastChunk = msg instanceof LastHttpContent;

            //initialize parser for current response
            if (!parser.isInitialized()) {
                parser.initialize(currentRequest(), responseContent, ResponseStatusConverter.fromHttp(responseHeader.getStatus().code()));
            }

            //important to place the RawQueryRequest test before, as it extends GenericQueryRequest
            if (currentRequest() instanceof RawQueryRequest) {
                response = handleRawQueryResponse(lastChunk, ctx);
            } else if (currentRequest() instanceof GenericQueryRequest) {
                response = parser.parse(lastChunk);
                if (lastChunk) {
                    parser.finishParsingAndReset();
                    finishedDecoding();
                }
            }
        }

        return response;
    }

    private RawQueryResponse handleRawQueryResponse(boolean lastChunk, ChannelHandlerContext ctx) {
        if (!lastChunk) {
            return null;
        }
        ResponseStatus status = ResponseStatusConverter.fromHttp(responseHeader.getStatus().code());
        ByteBuf responseCopy = ctx.alloc().buffer(responseContent.readableBytes(), responseContent.readableBytes());
        responseCopy.writeBytes(responseContent);

        return new RawQueryResponse(status, currentRequest(), responseCopy,
                responseHeader.getStatus().code(),
                responseHeader.getStatus().reasonPhrase());
    }

    @Override
    protected void finishedDecoding() {
        super.finishedDecoding();
        releaseResponseContent();
    }

    @Override
    public void handlerRemoved(final ChannelHandlerContext ctx) throws Exception {
        super.handlerRemoved(ctx);
        releaseResponseContent();
    }

    private void releaseResponseContent() {
        if (responseContent != null && responseContent.refCnt() > 0) {
            responseContent.release();
        }
        responseContent = null;
    }

    @Override
    protected CouchbaseRequest createKeepAliveRequest() {
        return new KeepAliveRequest();
    }

    protected static class KeepAliveRequest extends AbstractCouchbaseRequest implements QueryRequest, KeepAlive {
        protected KeepAliveRequest() {
            super(null, null);
        }
    }

    protected static class KeepAliveResponse extends AbstractCouchbaseResponse {
        protected KeepAliveResponse(ResponseStatus status, CouchbaseRequest request) {
            super(status, request);
        }
    }

    @Override
    protected ServiceType serviceType() {
        return ServiceType.QUERY;
    }
}