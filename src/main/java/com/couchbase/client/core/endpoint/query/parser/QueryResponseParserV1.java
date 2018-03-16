/*
 * Copyright (c) 2017 Couchbase, Inc.
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

package com.couchbase.client.core.endpoint.query.parser;
import com.couchbase.client.core.endpoint.util.ClosingPositionBufProcessor;
import com.couchbase.client.core.endpoint.util.StringClosingPositionBufProcessor;
import com.couchbase.client.core.endpoint.util.WhitespaceSkipper;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.query.GenericQueryResponse;
import com.couchbase.client.core.utils.UnicastAutoReleaseSubject;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufProcessor;
import rx.Scheduler;
import rx.subjects.AsyncSubject;

import java.util.concurrent.TimeUnit;

import static com.couchbase.client.core.endpoint.util.ByteBufJsonHelper.findNextChar;
import static com.couchbase.client.core.endpoint.util.ByteBufJsonHelper.findNextCharNotPrefixedBy;
import static com.couchbase.client.core.endpoint.util.ByteBufJsonHelper.findSectionClosingPosition;
import static com.couchbase.client.core.endpoint.util.ByteBufJsonHelper.findSplitPosition;

/**
 * Query response parser V1
 *
 * @author Michael Nitchinger
 * @author Simon Baslé
 *
 * @since 1.4.3
 */
public class QueryResponseParserV1 extends AbstractQueryResponseParser {
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(QueryResponseParserV1.class);

    //Query states
    protected static final byte QUERY_STATE_INITIAL = 0;
    protected static final byte QUERY_STATE_SIGNATURE = 1;
    protected static final byte QUERY_STATE_ROWS = 2;
    protected static final byte QUERY_STATE_ROWS_RAW = 20;
    protected static final byte QUERY_STATE_ROWS_DECIDE = 29;
    protected static final byte QUERY_STATE_ERROR = 3;
    protected static final byte QUERY_STATE_WARNING = 4;
    protected static final byte QUERY_STATE_STATUS = 5;
    protected static final byte QUERY_STATE_INFO = 6;
    protected static final byte QUERY_STATE_NO_INFO = 7; //alternate case where there's nothing after status
    protected static final byte QUERY_STATE_DONE = 8;

    /**
     * This is the number of characters expected to be present to be able to read
     * the beginning of the JSON, including the "requestID" token and its value
     * (currently expected to be 36 chars, but the code is adaptative).
     */
    private static final int MINIMUM_WINDOW_FOR_REQUESTID = 55;

    /**
     * This is a window of characters allowing to detect the clientContextID token
     * (including room for JSON separators, etc...).
     */
    public static final int MINIMUM_WINDOW_FOR_CLIENTID_TOKEN = 27;


    /**
     * Represents the current query parsing state.
     */
    private byte queryParsingState = QUERY_STATE_INITIAL;

    /**
     * In case of chunked processing, allows to detect we are still parsing a section.
     */
    private boolean sectionDone = false;

    private boolean sentResponse = false;

    public QueryResponseParserV1(Scheduler scheduler, long ttl) {
        super(scheduler, ttl);
    }

    public void initialize(CouchbaseRequest request, ByteBuf responseContent, ResponseStatus status) {
        this.currentRequest = request;
        this.responseContent = responseContent;
        this.status = status;
        this.initialized = true;
        this.sentResponse = false;
        this.response = null;
        queryParsingState = QUERY_STATE_INITIAL;
    }

    /**
     * Checks if there's not another section opened before the current one,
     * which starts at openBracketPos
     *
     * @param openBracketPos the position of the current section's opening bracket
     * @return true if transition to next state should be made because there's a new
     * section opening.
     */
    private boolean isEmptySection(int openBracketPos) {
        int nextColon = findNextChar(responseContent, ':');
        return nextColon > -1 && nextColon < openBracketPos;
    }


    public GenericQueryResponse parse(boolean lastChunk) {
        if (this.response == null) {
            handleGenericQueryResponse(lastChunk);
            if (this.response == null) {
                return null;
            }
        }
        parseQueryResponse(lastChunk);
        if (!this.sentResponse && this.response != null) {
            this.sentResponse = true;
            return this.response;
        }
        return null;
    }

    /**
     * Base method to handle the response for the generic query request.
     *
     * It waits for the first few bytes on the actual response to determine if an error is raised or if a successful
     * response can be expected. The actual error and/or chunk parsing is deferred to other parts of this handler.
     *
     * @return a {@link CouchbaseResponse} if eligible.
     */
    private void handleGenericQueryResponse(boolean lastChunk) {
        String requestId;
        String clientId = "";

        if (responseContent.readableBytes() < MINIMUM_WINDOW_FOR_REQUESTID + MINIMUM_WINDOW_FOR_CLIENTID_TOKEN
                && !lastChunk) {
            return; //wait for more data
        }

        int startIndex = responseContent.readerIndex();

        if (responseContent.readableBytes() >= MINIMUM_WINDOW_FOR_REQUESTID) {
            responseContent.skipBytes(findNextChar(responseContent, ':'));
            responseContent.skipBytes(findNextChar(responseContent, '"') + 1);
            int endOfId = findNextChar(responseContent, '"');
            ByteBuf slice = responseContent.readSlice(endOfId);
            requestId = slice.toString(CHARSET);
        } else {
            return;
        }

        //IMPORTANT: from there on, before returning null to get more data you need to reset
        //the cursor, since following code will consume data from the buffer.
        if (responseContent.readableBytes() >= MINIMUM_WINDOW_FOR_CLIENTID_TOKEN
                && findNextChar(responseContent, ':') < MINIMUM_WINDOW_FOR_CLIENTID_TOKEN) {
            responseContent.markReaderIndex();
            ByteBuf slice = responseContent.readSlice(findNextChar(responseContent, ':'));
            if (slice.toString(CHARSET).contains("clientContextID")) {
                //find the size of the client id
                responseContent.skipBytes(findNextChar(responseContent, '"') + 1); //opening of clientId
                int clientIdSize = findNextCharNotPrefixedBy(responseContent, '"', '\\');
                if (clientIdSize < 0) {
                    //reset the cursor way back before requestID, there was not enough data to get the whole id
                    responseContent.readerIndex(startIndex);
                    //wait for more data
                    return;
                }
                //read it
                clientId = responseContent.readSlice(clientIdSize).toString(CHARSET);
                //advance to next token if possible
                //closing quote
                boolean hasClosingQuote = responseContent.readableBytes() > 0;
                if (hasClosingQuote) {
                    responseContent.skipBytes(1);
                }
                //next token's quote
                int openingNextToken = findNextChar(responseContent, '"');
                if (openingNextToken > -1) {
                    responseContent.skipBytes(openingNextToken);
                }
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
            //it is important to reset the readerIndex if returning null, in order to allow for complete retry
            responseContent.readerIndex(startIndex);
            return;
        }

        if (!success) {
            status = ResponseStatus.FAILURE;
        }

        queryRowObservable = UnicastAutoReleaseSubject.create(this.ttl, TimeUnit.MILLISECONDS, this.scheduler);
        queryErrorObservable = UnicastAutoReleaseSubject.create(this.ttl, TimeUnit.MILLISECONDS, this.scheduler);
        queryStatusObservable = AsyncSubject.create();
        queryInfoObservable = UnicastAutoReleaseSubject.create(this.ttl, TimeUnit.MILLISECONDS, this.scheduler);
        querySignatureObservable = UnicastAutoReleaseSubject.create(ttl, TimeUnit.MILLISECONDS, this.scheduler);

        //set up trace ids on all these UnicastAutoReleaseSubjects, so that if they get in a bad state
        // (multiple subscribers or subscriber coming in too late) we can trace back to here
        String rid = clientId == null ? requestId : clientId + " / " + requestId;
        queryRowObservable.withTraceIdentifier("queryRow." + rid);
        queryErrorObservable.withTraceIdentifier("queryError." + rid);
        queryInfoObservable.withTraceIdentifier("queryInfo." + rid);
        querySignatureObservable.withTraceIdentifier("querySignature." + rid);

        this.response = new GenericQueryResponse(
                queryErrorObservable.onBackpressureBuffer().observeOn(scheduler),
                queryRowObservable.onBackpressureBuffer().observeOn(scheduler),
                querySignatureObservable.onBackpressureBuffer().observeOn(scheduler),
                queryStatusObservable.onBackpressureBuffer().observeOn(scheduler),
                queryInfoObservable.onBackpressureBuffer().observeOn(scheduler),
                this.currentRequest,
                this.status, requestId, clientId);
    }

    /**
     * Generic dispatch method to parse the query response chunks.
     *
     * Depending on the state the parser is currently in, several different sub-methods are called
     * which do the actual handling.
     *
     * @param lastChunk if the current emitted content body is the last one.
     */
    private void parseQueryResponse(boolean lastChunk) {
        if (sectionDone || queryParsingState == QUERY_STATE_INITIAL) {
            queryParsingState = transitionToNextToken(lastChunk);
        }

        if (queryParsingState == QUERY_STATE_SIGNATURE) {
            parseQuerySignature(lastChunk);
        }

        if (queryParsingState == QUERY_STATE_ROWS_DECIDE) {
            decideBetweenRawAndObjects(lastChunk);
        }
        if (queryParsingState == QUERY_STATE_ROWS) {
            parseQueryRows(lastChunk);
        } else if (queryParsingState == QUERY_STATE_ROWS_RAW) {
            parseQueryRowsRaw(lastChunk);
        }

        if (queryParsingState == QUERY_STATE_ERROR) {
            parseQueryError(lastChunk);
        }

        if (queryParsingState == QUERY_STATE_WARNING) {
            parseQueryError(lastChunk); //warning are treated the same as errors -> sent to errorObservable
        }

        if (queryParsingState == QUERY_STATE_STATUS) {
            parseQueryStatus(lastChunk);
        }

        if (queryParsingState == QUERY_STATE_INFO) {
            parseQueryInfo(lastChunk);
        } else if (queryParsingState == QUERY_STATE_NO_INFO) {
            finishInfo();
        }
    }

    /**
     * Peek the next token, returning the QUERY_STATE corresponding to it and placing the readerIndex just after
     * the token's ':'. Must be at the end of the previous token.
     *
     * @param lastChunk true if this is the last chunk
     * @return the next QUERY_STATE
     */
    private byte transitionToNextToken(boolean lastChunk) {
        int endNextToken = findNextChar(responseContent, ':');
        if (endNextToken < 0 && !lastChunk) {
            return queryParsingState;
        }

        if (endNextToken < 0 && lastChunk && queryParsingState >= QUERY_STATE_STATUS) {
            return QUERY_STATE_NO_INFO;
        }

        byte newState;
        ByteBuf peekSlice = responseContent.readSlice(endNextToken + 1);
        String peek = peekSlice.toString(CHARSET);
        if (peek.contains("\"signature\":")) {
            newState = QUERY_STATE_SIGNATURE;
        } else if (peek.endsWith("\"results\":")) {
            newState = QUERY_STATE_ROWS_DECIDE;
        } else if (peek.endsWith("\"status\":")) {
            newState = QUERY_STATE_STATUS;
        } else if (peek.endsWith("\"errors\":")) {
            newState = QUERY_STATE_ERROR;
        } else if (peek.endsWith("\"warnings\":")) {
            newState = QUERY_STATE_WARNING;
        } else if (peek.endsWith("\"metrics\":")) {
            newState = QUERY_STATE_INFO;
        } else {
            if (lastChunk) {
                IllegalStateException e = new IllegalStateException("Error parsing query response (in TRANSITION) at \""
                        + peek + "\", enable trace to see response content");
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace(responseContent.toString(CHARSET), e);
                }
                throw e;
            } else {
                //we need more data
                return queryParsingState;
            }
        }

        sectionDone = false;
        return newState;
    }

    private void decideBetweenRawAndObjects(boolean lastChunk) {
        responseContent.markReaderIndex();
        int openArrayPos = findNextChar(responseContent, '[');
        if (openArrayPos > -1) {
            responseContent.skipBytes(openArrayPos + 1);
        } else {
            responseContent.resetReaderIndex();
            if (lastChunk == true) {
                throw new IllegalStateException("Unable to decide between raw and objects with content " + responseContent.toString(CHARSET));
            }
            return; //more data
        }

        int spaceToSkip = responseContent.forEachByte(new WhitespaceSkipper());
        if (spaceToSkip > -1) {
            responseContent.readerIndex(spaceToSkip);
        } else {
            //there's only whitespace! need more info
            responseContent.resetReaderIndex();
            return;
        }

        if (responseContent.isReadable()) {
            byte first = responseContent.getByte(responseContent.readerIndex());
            if (first == '{') {
                queryParsingState = QUERY_STATE_ROWS;
            } else if (first == ']') {
                //empty result section!
                sectionDone();
                queryParsingState = transitionToNextToken(lastChunk);
            } else {
                queryParsingState = QUERY_STATE_ROWS_RAW;
            }
        } else {
            responseContent.resetReaderIndex();
        }
    }

    private void sectionDone() {
        this.sectionDone = true;
        responseContent.discardReadBytes();
    }

    /**
     * Parse the signature section in the N1QL response.
     */
    private void parseQuerySignature(boolean lastChunk) {
        ByteBufProcessor processor = null;
        //signature can be any valid JSON item, which get tricky to detect
        //let's try to find out what's the boundary character
        int openPos = responseContent.forEachByte(new WhitespaceSkipper()) - responseContent.readerIndex();
        if (openPos < 0) {
            //only whitespace left in the buffer, need more data
            return;
        }
        char openChar = (char) responseContent.getByte(responseContent.readerIndex() + openPos);
        if (openChar == '{') {
            processor = new ClosingPositionBufProcessor('{', '}', true);
        } else if (openChar == '[') {
            processor = new ClosingPositionBufProcessor('[', ']', true);
        } else if (openChar == '"') {
            processor = new StringClosingPositionBufProcessor();
        } //else this should be a scalar, skip processor

        int closePos;
        if (processor != null) {
            closePos = responseContent.forEachByte(processor) - responseContent.readerIndex();
        } else {
            closePos = findNextChar(responseContent, ',') - 1;
        }
        if (closePos > 0) {
            responseContent.skipBytes(openPos);
            int length = closePos - openPos + 1;
            ByteBuf signature = responseContent.readSlice(length);
            querySignatureObservable.onNext(signature.copy());
        } else {
            //wait for more data
            return;
        }
        //note: the signature section could be absent, so we'll make sure to complete the observable
        // when receiving status since this is in every well-formed response.
        sectionDone();
        queryParsingState = transitionToNextToken(lastChunk);
    }

    /**
     * Parses the query rows from the content stream as long as there is data to be found.
     */
    private void parseQueryRows(boolean lastChunk) {
        while (true) {
            int openBracketPos = findNextChar(responseContent, '{');
            if (isEmptySection(openBracketPos) || (lastChunk && openBracketPos < 0)) {
                sectionDone();
                queryParsingState = transitionToNextToken(lastChunk);
                break;
            }

            int closeBracketPos = findSectionClosingPosition(responseContent, '{', '}');
            if (closeBracketPos == -1) {
                break;
            }

            int length = closeBracketPos - openBracketPos - responseContent.readerIndex() + 1;
            responseContent.skipBytes(openBracketPos);
            ByteBuf resultSlice = responseContent.readSlice(length);
            queryRowObservable.onNext(resultSlice.copy());
            responseContent.discardSomeReadBytes();
        }
    }

    /**
     * Parses the query raw results from the content stream as long as there is data to be found.
     */
    private void parseQueryRowsRaw(boolean lastChunk) {
        while (responseContent.isReadable()) {
            int splitPos = findSplitPosition(responseContent, ',');
            int arrayEndPos = findSplitPosition(responseContent, ']');

            boolean doSectionDone = false;

            if (splitPos == -1 && arrayEndPos == -1) {
                //need more data
                break;
            } else if (arrayEndPos > 0 && (arrayEndPos < splitPos || splitPos == -1)) {
                splitPos = arrayEndPos;
                doSectionDone = true;
            }

            int length = splitPos - responseContent.readerIndex();
            ByteBuf resultSlice = responseContent.readSlice(length);
            queryRowObservable.onNext(resultSlice.copy());
            responseContent.skipBytes(1);
            responseContent.discardReadBytes();

            if (doSectionDone) {
                sectionDone();
                queryParsingState = transitionToNextToken(lastChunk);
                break;
            }
        }
    }

    /**
     * Parses the errors and warnings from the content stream as long as there are some to be found.
     */
    private void parseQueryError(boolean lastChunk) {
        while (true) {
            int openBracketPos = findNextChar(responseContent, '{');
            if (isEmptySection(openBracketPos) || (lastChunk && openBracketPos < 0)) {
                sectionDone();
                queryParsingState = transitionToNextToken(lastChunk); //warnings or status
                break;
            }

            int closeBracketPos = findSectionClosingPosition(responseContent, '{', '}');
            if (closeBracketPos == -1) {
                break;
            }

            int length = closeBracketPos - openBracketPos - responseContent.readerIndex() + 1;
            responseContent.skipBytes(openBracketPos);
            ByteBuf resultSlice = responseContent.readSlice(length);
            queryErrorObservable.onNext(resultSlice.copy());
        }
    }

    /**
     * Last before the end of the stream, we can now parse the final result status
     * (including full execution of the query).
     */
    private void parseQueryStatus(boolean lastChunk) {
        //some sections don't always come up, unlike status. Take this chance to close said sections' observables here.
        querySignatureObservable.onCompleted();
        queryRowObservable.onCompleted();
        queryErrorObservable.onCompleted();

        responseContent.markReaderIndex();
        responseContent.skipBytes(findNextChar(responseContent, '"') + 1);
        int endStatus = findNextChar(responseContent, '"');
        if (endStatus > -1) {
            ByteBuf resultSlice = responseContent.readSlice(endStatus);
            queryStatusObservable.onNext(resultSlice.toString(CHARSET));
            queryStatusObservable.onCompleted();
            sectionDone();
            queryParsingState = transitionToNextToken(lastChunk);
        } else {
            responseContent.resetReaderIndex();
            return; //need more data
        }
    }

    private void finishInfo() {
        queryInfoObservable.onCompleted();
        sectionDone();
        queryParsingState = QUERY_STATE_DONE;
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
        int openBracketPos = findNextChar(responseContent, '{');
        int closeBracketPos = findSectionClosingPosition(responseContent, '{', '}');
        if (closeBracketPos == -1) {
            if (last) {
                throw new IllegalStateException("Could not find metrics closing in last chunk");
            } else {
                return; //wait for more data
            }
        }

        int from = responseContent.readerIndex() + openBracketPos;
        int to = closeBracketPos - openBracketPos - responseContent.readerIndex() + 1;
        queryInfoObservable.onNext(responseContent.slice(from, to).copy());
        responseContent.readerIndex(to + openBracketPos);

        //has to be here rather than in parseQueryResponse, as when there is a split
        //(and thus not enough data) it could finish the metrics too early
        finishInfo();
    }
}