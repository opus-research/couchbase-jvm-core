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

import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.ResponseStatus;

import com.couchbase.client.core.message.query.GenericQueryResponse;
import com.couchbase.client.core.utils.UnicastAutoReleaseSubject;
import com.github.subalakr.yasjl.ByteBufJsonParser;
import com.github.subalakr.yasjl.Callbacks.JsonPointerCB1;
import com.github.subalakr.yasjl.JsonPointer;

import java.io.EOFException;

import io.netty.buffer.ByteBuf;
import rx.Scheduler;
import rx.subjects.AsyncSubject;

import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

/**
 * yasjl based query response parser
 *
 * @author Subhashni Balakrishnan
 * @since 1.4.3
 */
public class QueryResponseParserV2 extends AbstractQueryResponseParser {

    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(QueryResponseParserV2.class);

    final private ByteBufJsonParser parser;
    private String requestID;
    private String clientContextID;
    private boolean sentResponse;

    public QueryResponseParserV2(Scheduler scheduler, long ttl) {
        super(scheduler, ttl);
        this.parser = new ByteBufJsonParser();
    }

    public void initialize(CouchbaseRequest request, ByteBuf responseContent, final ResponseStatus responseStatus) {
        this.requestID = "";
        this.clientContextID = ""; //initialize to empty strings instead of null as we may not receive context id sometimes
        this.sentResponse = false;
        this.response = null;
        this.status = responseStatus;

        JsonPointer[] jsonPointers = {
                new JsonPointer("/requestID", new JsonPointerCB1() {
                    public void call(ByteBuf buf) {
                        requestID = buf.toString(Charset.forName("UTF-8"));
                        requestID = requestID.substring(1, requestID.length() - 1);
                        buf.release();

                        queryRowObservable = UnicastAutoReleaseSubject.create(ttl, TimeUnit.MILLISECONDS, scheduler);
                        queryErrorObservable = UnicastAutoReleaseSubject.create(ttl, TimeUnit.MILLISECONDS, scheduler);
                        queryStatusObservable = AsyncSubject.create();
                        queryInfoObservable = UnicastAutoReleaseSubject.create(ttl, TimeUnit.MILLISECONDS, scheduler);
                        querySignatureObservable = UnicastAutoReleaseSubject.create(ttl, TimeUnit.MILLISECONDS, scheduler);
                        queryRowObservable.withTraceIdentifier("queryRow." + requestID);
                        queryErrorObservable.withTraceIdentifier("queryError." + requestID);
                        queryInfoObservable.withTraceIdentifier("queryInfo." + requestID);
                        querySignatureObservable.withTraceIdentifier("querySignature." + requestID);
                    }
                }),

                new JsonPointer("/clientContextID", new JsonPointerCB1() {
                    public void call(ByteBuf buf) {
                        clientContextID = buf.toString(Charset.forName("UTF-8"));
                        clientContextID = clientContextID.substring(1, clientContextID.length() - 1);
                        buf.release();
                    }
                }),
                new JsonPointer("/signature", new JsonPointerCB1() {
                    public void call(ByteBuf buf) {
                        if (querySignatureObservable != null) {
                            querySignatureObservable.onNext(buf);
                        }
                    }
                }),
                new JsonPointer("/status", new JsonPointerCB1() {
                    public void call(ByteBuf buf) {
                        if (queryStatusObservable != null) {
                            String statusStr = buf.toString(Charset.forName("UTF-8"));
                            buf.release();

                            statusStr = statusStr.substring(1, statusStr.length() - 1);
                            if (!statusStr.equals("success")) {
                                status = ResponseStatus.FAILURE;
                            }
                            queryStatusObservable.onNext(statusStr);

                            //overwrite existing response object if streamed in status
                            if (response == null || !sentResponse) {
                                createResponse();
                            }
                        }
                    }
                }),
                new JsonPointer("/metrics", new JsonPointerCB1() {
                    public void call(ByteBuf buf) {
                        if (queryInfoObservable != null) {
                            queryInfoObservable.onNext(buf);
                        }
                    }
                }),
                new JsonPointer("/results/-", new JsonPointerCB1() {
                    public void call(ByteBuf buf) {
                        if (queryRowObservable != null) {
                            queryRowObservable.onNext(buf);
                            if (response == null) {
                                createResponse();
                            }
                        }
                    }
                }),
                new JsonPointer("/errors/-", new JsonPointerCB1() {
                    public void call(ByteBuf buf) {
                        if (queryErrorObservable != null) {
                            queryErrorObservable.onNext(buf);
                            if (response == null) {
                                createResponse();
                            }
                        }
                    }
                }),
                new JsonPointer("/warnings/-", new JsonPointerCB1() {
                    public void call(ByteBuf buf) {
                        if (queryErrorObservable != null) {
                            queryErrorObservable.onNext(buf);
                            if (response == null) {
                                createResponse();
                            }
                        }
                    }
                }),
        };
        this.responseContent = responseContent;
        try {
            parser.initialize(responseContent, jsonPointers);
        } catch (Exception e) {

        }
        initialized = true;
    }

    private void createResponse() {
        //when streaming results/errors/status starts, build out the response
        response = new GenericQueryResponse(
                queryErrorObservable.onBackpressureBuffer().observeOn(scheduler),
                queryRowObservable.onBackpressureBuffer().observeOn(scheduler),
                querySignatureObservable.onBackpressureBuffer().observeOn(scheduler),
                queryStatusObservable.onBackpressureBuffer().observeOn(scheduler),
                queryInfoObservable.onBackpressureBuffer().observeOn(scheduler),
                currentRequest,
                status, requestID, clientContextID);
    }

    //parses the response content
    public GenericQueryResponse parse(boolean lastChunk) throws Exception {
        try {
            parser.parse();
            //discard only if EOF is not thrown
            responseContent.discardSomeReadBytes();
        } catch (EOFException ex) {
            //ignore as we except chunked responses
        }

        //return back response only once
        if (!this.sentResponse && this.response != null) {
            this.sentResponse = true;
            return this.response;
        }
        return null;
    }
}