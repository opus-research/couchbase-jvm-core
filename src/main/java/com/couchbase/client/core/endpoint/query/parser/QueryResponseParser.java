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
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.query.GenericQueryResponse;
import io.netty.buffer.ByteBuf;

/**
 * @author Subhashni Balakrishnan
 * @since 1.4.3
 */
public interface QueryResponseParser {

    //initialize with current request, response content
    void initialize(CouchbaseRequest request, ByteBuf responseContent, ResponseStatus status);

    //parses the response content
    GenericQueryResponse parse(boolean lastChunk) throws Exception;

    //finish parsing and emit completed event to response observables
    void finishParsingAndReset();

    //check if the parser is initialized
    boolean isInitialized();
}