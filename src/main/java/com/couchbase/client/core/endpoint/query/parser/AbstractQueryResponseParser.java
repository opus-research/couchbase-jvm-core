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

import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.query.GenericQueryResponse;
import com.couchbase.client.core.utils.UnicastAutoReleaseSubject;
import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;
import rx.Scheduler;
import rx.subjects.AsyncSubject;

import java.nio.charset.Charset;

/**
 * @author Simon Baslé
 * @author Subhashni Balakrishnan
 * @since 1.4.3
 */
public abstract class AbstractQueryResponseParser implements QueryResponseParser {

    protected static final Charset CHARSET = CharsetUtil.UTF_8;

    protected ByteBuf responseContent;
    /**
     * Represents an observable that sends result chunks.
     */
    protected UnicastAutoReleaseSubject<ByteBuf> queryRowObservable;

    /**
     * Represents an observable that has the signature of the N1QL results if there are any.
     */
    protected UnicastAutoReleaseSubject<ByteBuf> querySignatureObservable;

    /**
     * Represents an observable that sends errors and warnings if any during query execution.
     */
    protected UnicastAutoReleaseSubject<ByteBuf> queryErrorObservable;

    /**
     * Represent an observable that has the final execution status of the query, once all result rows and/or
     * errors/warnings have been sent.
     */
    protected AsyncSubject<String> queryStatusObservable;

    /**
     * Represents an observable containing metrics on a terminated query.
     */
    protected UnicastAutoReleaseSubject<ByteBuf> queryInfoObservable;

    /**
     * Represents the current request
     */
    protected CouchbaseRequest currentRequest;

    /**
     * Scheduler for query response
     */
    protected Scheduler scheduler;

    /**
     * TTL for response observables
     */
    protected long ttl;

    /**
     * Response status
     */
    protected ResponseStatus status;

    /**
     * Flag to indicate if the parser is initialized
     */
    protected boolean initialized;

    /**
     * Response that should be returned on parse call
     */
    protected GenericQueryResponse response;

    public AbstractQueryResponseParser(Scheduler scheduler, long ttl) {
        this.scheduler = scheduler;
        this.ttl = ttl;
        this.response = null;
    }

    public boolean isInitialized() {
        return this.initialized;
    }


    //finish parsing and emit completed event to response observables
    public void finishParsingAndReset() {
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
        if (querySignatureObservable != null) {
            querySignatureObservable.onCompleted();
        }
        queryInfoObservable = null;
        queryRowObservable = null;
        queryErrorObservable = null;
        queryStatusObservable = null;
        querySignatureObservable = null;
        this.initialized = false;
    }
}
