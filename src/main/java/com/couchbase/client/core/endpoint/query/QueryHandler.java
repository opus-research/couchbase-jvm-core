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
package com.couchbase.client.core.endpoint.query;

import java.util.Queue;

import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.endpoint.AbstractEndpoint;
import com.couchbase.client.core.endpoint.AbstractGenericHandler;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.query.GenericQueryRequest;
import com.couchbase.client.core.message.query.GenericQueryResponse;
import com.couchbase.client.core.message.query.QueryRequest;
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
import rx.Scheduler;
import rx.subjects.ReplaySubject;

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

    private static final byte QUERY_STATE_INITIAL = 0;
    private static final byte QUERY_STATE_SIGNATURE = 1;
    private static final byte QUERY_STATE_ROWS = 2;
    private static final byte QUERY_STATE_ERROR = 3;
    private static final byte QUERY_STATE_WARNING = 4;
    private static final byte QUERY_STATE_STATUS = 5;
    private static final byte QUERY_STATE_INFO = 6;
    private static final byte QUERY_STATE_DONE = 7;

    /**
     * Contains the current pending response header if set.
     */
    private HttpResponse responseHeader;

    /**
     * Contains the accumulating buffer for the response content.
     */
    private ByteBuf responseContent;

    /**
     * Represents a observable that sends result chunks.
     */
    private ReplaySubject<ByteBuf> queryRowObservable;

    /**
     * Represents an observable that sends errors and warnings if any during query execution.
     */
    private ReplaySubject<ByteBuf> queryErrorObservable;

    /**
     * Represent an observable that has the final execution status of the query, once all result rows and/or
     * errors/warnings have been sent.
     */
    private ReplaySubject<String> queryStatusObservable;

    /**
     * Represents a observable containing metrics on a terminated query.
     */
    private ReplaySubject<ByteBuf> queryInfoObservable;

    /**
     * Represents the current query parsing state.
     */
    private byte queryParsingState = QUERY_STATE_INITIAL;

    /**
     * Creates a new {@link QueryHandler} with the default queue for requests.
     *
     * @param endpoint the {@link AbstractEndpoint} to coordinate with.
     * @param responseBuffer the {@link RingBuffer} to push responses into.
     */
    public QueryHandler(AbstractEndpoint endpoint, RingBuffer<ResponseEvent> responseBuffer) {
        super(endpoint, responseBuffer);
    }

    /**
     * Creates a new {@link QueryHandler} with a custom queue for requests (suitable for tests).
     *
     * @param endpoint the {@link AbstractEndpoint} to coordinate with.
     * @param responseBuffer the {@link RingBuffer} to push responses into.
     * @param queue the queue which holds all outstanding open requests.
     */
    QueryHandler(AbstractEndpoint endpoint, RingBuffer<ResponseEvent> responseBuffer, Queue<QueryRequest> queue) {
        super(endpoint, responseBuffer, queue);
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
            request.content().writeBytes(query);
            query.release();
        } else {
            throw new IllegalArgumentException("Unknown incoming QueryRequest type "
                + msg.getClass());
        }

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

        if (msg instanceof HttpContent) {
            responseContent.writeBytes(((HttpContent) msg).content());

            if (currentRequest() instanceof GenericQueryRequest) {
                if (queryRowObservable == null) {
                    response = handleGenericQueryResponse();
                }

                parseQueryResponse(msg instanceof LastHttpContent);
            }
        }

        return response;
    }

    private int bytesBefore(char c) {
        return responseContent.bytesBefore((byte) c);
    }

    /**
     * Base method to handle the response for the generic query request.
     *
     * It waits for the first few bytes on the actual response to determine if an error is raised or if a successful
     * response can be expected. The actual error and/or chunk parsing is deferred to other parts of this handler.
     *
     * @return a {@link CouchbaseResponse} if eligible.
     */
    private CouchbaseResponse handleGenericQueryResponse() {
        String requestId;
        String clientId = "";

        if (responseContent.readableBytes() >= 55) {
            responseContent.skipBytes(bytesBefore(':'));
            responseContent.skipBytes(bytesBefore('"') + 1);
            ByteBuf slice = responseContent.readSlice(36);
            requestId = slice.toString(CHARSET);
        } else {
            return null;
        }

        if (responseContent.readableBytes() >= 27 && bytesBefore(':') < 27) {
            responseContent.markReaderIndex();
            ByteBuf slice = responseContent.readSlice(bytesBefore(':'));
            if (slice.toString(CHARSET).contains("clientContextID")) {
                //find the size of the client id
                responseContent.skipBytes(bytesBefore('"') + 1); //opening of clientId
                //TODO this doesn't account for the fact that the id can contain an escaped " !!!
                int clientIdSize = bytesBefore('"');
                if (clientIdSize < 0) {
                    return null;
                }
                //read it
                clientId = responseContent.readSlice(clientIdSize).toString(CHARSET);
                //advance to next token
                responseContent.skipBytes(1);//closing quote
                responseContent.skipBytes(bytesBefore('"')); //next token's quote
            } else {
                //reset the cursor, there was no client id
                responseContent.resetReaderIndex();
            }
        }

        boolean success = true;
        if (responseContent.readableBytes() >= 20) {
            ByteBuf peekForErrors = responseContent.slice(responseContent.readerIndex(), 20);
            if (peekForErrors.toString(CHARSET).contains("errors")) {
                success = false;
            }
        } else {
            return null;
        }

        ResponseStatus status = statusFromCode(responseHeader.getStatus().code());
        if (!success) {
            status = ResponseStatus.FAILURE;
        }

        Scheduler scheduler = env().scheduler();
        queryRowObservable = ReplaySubject.create();
        queryErrorObservable = ReplaySubject.create();
        queryStatusObservable = ReplaySubject.create();
        queryInfoObservable = ReplaySubject.create();
        return new GenericQueryResponse(
                queryErrorObservable.onBackpressureBuffer().observeOn(scheduler),
                queryRowObservable.onBackpressureBuffer().observeOn(scheduler),
                queryStatusObservable.onBackpressureBuffer().observeOn(scheduler),
                queryInfoObservable.onBackpressureBuffer().observeOn(scheduler),
                currentRequest(),
                status, requestId, clientId
        );
    }

    /**
     * Generic dispatch method to parse the query response chunks.
     *
     * Depending on the state the parser is currently in, several different sub-methods are caleld which do the actual
     * handling.
     *
     * @param lastChunk if the current emitted content body is the last one.
     */
    private void parseQueryResponse(boolean lastChunk) {
        if (queryParsingState == QUERY_STATE_INITIAL) {
            queryParsingState = transitionToNextToken();
        }

        if (queryParsingState == QUERY_STATE_SIGNATURE) {
            parseQuerySignature();
        }

        if (queryParsingState == QUERY_STATE_ROWS) {
            parseQueryRows();
        }

        if (queryParsingState == QUERY_STATE_ERROR) {
            parseQueryError();
        }

        if (queryParsingState == QUERY_STATE_WARNING) {
            parseQueryError(); //warning are treated the same as errors -> sent to errorObservable
        }

        if (queryParsingState == QUERY_STATE_STATUS) {
            parseQueryStatus();
        }

        if (queryParsingState == QUERY_STATE_INFO) {
            parseQueryInfo(lastChunk);
        }

        if (queryParsingState == QUERY_STATE_DONE) {
            cleanupQueryStates();
        }
    }

    /**
     * Peek the next token, returning the QUERY_STATE corresponding to it and placing the readerIndex just after
     * the token's ':'. Must be at the end of the previous token.
     *
     * @returns the next QUERY_STATE
     */
    private byte transitionToNextToken() {
        int endNextToken = responseContent.bytesBefore((byte) ':');
        ByteBuf peekSlice = responseContent.readSlice(endNextToken + 1);
        String peek = peekSlice.toString(CHARSET);
        if (peek.contains("\"signature\"")) {
            return QUERY_STATE_SIGNATURE;
        } else if (peek.contains("\"results\"")) {
            return QUERY_STATE_ROWS;
        } else if (peek.contains("\"status\"")) {
            return QUERY_STATE_STATUS;
        } else if (peek.contains("\"errors\"")) {
            return QUERY_STATE_ERROR;
        } else if (peek.contains("\"warnings\"")) {
            return QUERY_STATE_WARNING;
        } else if (peek.contains("\"metrics\"")) {
            return QUERY_STATE_INFO;
        } else {
            IllegalStateException e = new IllegalStateException("Error parsing query response (in TRANSITION) at " + peek);
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(e);
                LOGGER.trace(responseContent.toString(CHARSET));
            }
            throw e;
        }
    }

    private int findEnd(char openingChar, char closingChar) {
        int closePos = -1;
        int openCount = 0;
        for (int i = responseContent.readerIndex(); i <= responseContent.writerIndex(); i++) {
            byte current = responseContent.getByte(i);
            if (current == openingChar) {
                openCount++;
            } else if (current == closingChar && openCount > 0) {
                openCount--;
                if (openCount == 0) {
                    closePos = i;
                    break;
                }
            }
        }
        return closePos;
    }

    /**
     * For now skip the signature.
     */
    private void parseQuerySignature() {
        int openPos = responseContent.bytesBefore((byte) '{');
        int closePos = findEnd('{', '}');
        if (closePos > 0) {
            int length = closePos - openPos - responseContent.readerIndex() + 1;
            responseContent.skipBytes(openPos);
            ByteBuf signature = responseContent.readSlice(length);
        }
        queryParsingState = transitionToNextToken();
    }

    /**
     * Parses the query rows from the content stream as long as there is data to be found.
     */
    private void parseQueryRows() {
        while (true) {
            int openBracketPos = responseContent.bytesBefore((byte) '{');
            int nextColonPos = responseContent.bytesBefore((byte) ':');
            if (nextColonPos < openBracketPos) {
                queryParsingState = transitionToNextToken();
                break;
            }

            int closeBracketPos = findEnd('{', '}');
            if (closeBracketPos == -1) {
                break;
            }

            int length = closeBracketPos - openBracketPos - responseContent.readerIndex() + 1;
            responseContent.skipBytes(openBracketPos);
            ByteBuf resultSlice = responseContent.readSlice(length);
            queryRowObservable.onNext(resultSlice.copy());
        }

        responseContent.discardReadBytes();
    }

    /**
     * Parses the errors and warnings from the content stream as long as there are some to be found.
     */
    private void parseQueryError() {
        while (true) {
            int openBracketPos = responseContent.bytesBefore((byte) '{');
            int nextColonPos = responseContent.bytesBefore((byte) ':');
            if (nextColonPos < openBracketPos) {
                queryParsingState = transitionToNextToken(); //warnings or status
                break;
            }

            int closeBracketPos = findEnd('{', '}');
            if (closeBracketPos == -1) {
                break;
            }

            int length = closeBracketPos - openBracketPos - responseContent.readerIndex() + 1;
            responseContent.skipBytes(openBracketPos);
            ByteBuf resultSlice = responseContent.readSlice(length);
            queryErrorObservable.onNext(resultSlice.copy());
        }

        responseContent.discardReadBytes();
    }

    /**
     * Last before the end of the stream, we can now parse the final result status
     * (including full execution of the query).
     */
    private void parseQueryStatus() {
        queryRowObservable.onCompleted();
        queryErrorObservable.onCompleted();

        responseContent.skipBytes(responseContent.bytesBefore((byte) '"') + 1);
        ByteBuf resultSlice = responseContent.readSlice(responseContent.bytesBefore((byte) '"'));
        queryStatusObservable.onNext(resultSlice.toString(CHARSET));
        queryStatusObservable.onCompleted();
        queryParsingState = transitionToNextToken();
    }

    /**
     * At the end of the response stream, parse out the info portion (metrics).
     *
     * For the sake of easiness, since we know it comes at the end, we wait until the full data is together and read
     * the info json objects off in one shot (but they are still emitted separately).
     *
     * @param last if this batch is the last one.
     */
    private void parseQueryInfo(boolean last) {
        if (!last) {
            return;
        }

        int initColon = responseContent.bytesBefore((byte) ':');
        responseContent.readerIndex(initColon);

        while (true) {
            int openBracketPos = responseContent.bytesBefore((byte) '{');
            int closeBracketPos = -1;
            int openBrackets = 0;
            for (int i = responseContent.readerIndex(); i <= responseContent.writerIndex(); i++) {
                byte current = responseContent.getByte(i);
                if (current == '{') {
                    openBrackets++;
                } else if (current == '}' && openBrackets > 0) {
                    openBrackets--;
                    if (openBrackets == 0) {
                        closeBracketPos = i;
                        break;
                    }
                }
            }

            if (closeBracketPos == -1) {
                break;
            }

            int from = responseContent.readerIndex() + openBracketPos;
            int to = closeBracketPos - openBracketPos - responseContent.readerIndex() + 1;
            queryInfoObservable.onNext(responseContent.slice(from, to).copy());
            responseContent.readerIndex(to + openBracketPos);
        }

        queryInfoObservable.onCompleted();
        queryParsingState = QUERY_STATE_DONE;
    }

    /**
     * Clean up the query states after all rows have been consumed.
     */
    private void cleanupQueryStates() {
        finishedDecoding();
        queryInfoObservable = null;
        queryRowObservable = null;
        queryErrorObservable = null;
        queryStatusObservable = null;
        queryParsingState = QUERY_STATE_INITIAL;
    }

    /**
     * Converts a HTTP status code in its appropriate {@link ResponseStatus} representation.
     *
     * @param code the http code.
     * @return the parsed status.
     */
    private static ResponseStatus statusFromCode(int code) {
        ResponseStatus status;
        switch (code) {
            case 200:
            case 201:
                status = ResponseStatus.SUCCESS;
                break;
            case 404:
                status = ResponseStatus.NOT_EXISTS;
                break;
            default:
                status = ResponseStatus.FAILURE;
        }
        return status;
    }

    @Override
    public void handlerRemoved(final ChannelHandlerContext ctx) throws Exception {
        if (queryRowObservable != null) {
            queryRowObservable.onCompleted();
        }
        if (queryInfoObservable != null) {
            queryInfoObservable.onCompleted();
        }
        if (queryErrorObservable != null) {
            queryErrorObservable.onCompleted();
        }
        if (queryStatusObservable != null) {
            queryStatusObservable.onCompleted();
        }
        cleanupQueryStates();
        super.handlerRemoved(ctx);
    }


}
