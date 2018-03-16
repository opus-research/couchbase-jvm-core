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

package com.couchbase.client.core.message.config;

import com.couchbase.client.core.endpoint.ResponseStatusConverter;
import com.couchbase.client.core.message.AbstractCouchbaseResponse;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.config.RestApiRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Short description of class
 *
 * @author Simon Baslé
 * @since X.X
 */
public class RestApiResponse extends AbstractCouchbaseResponse {

    private final HttpResponseStatus httpStatus;
    private final String body;

    public RestApiResponse(RestApiRequest request, HttpResponseStatus status, String body) {
        super(ResponseStatusConverter.fromHttp(status.code()), request);
        this.httpStatus = status;
        this.body = body;
    }

    @Override
    public RestApiRequest request() {
        return (RestApiRequest) super.request();
    }

    public HttpResponseStatus httpStatus() {
        return httpStatus;
    }

    public String body() {
        return body;
    }
}
