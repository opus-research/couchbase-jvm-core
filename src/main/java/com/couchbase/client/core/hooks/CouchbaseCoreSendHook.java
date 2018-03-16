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
package com.couchbase.client.core.hooks;

import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.lang.Tuple2;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.CouchbaseCore;
import com.couchbase.client.core.message.CouchbaseResponse;
import rx.subjects.Subject;

/**
 * This hook allows to intercept and modify the {@link CouchbaseRequest} in the
 * path of the {@link CouchbaseCore#send(CouchbaseRequest)} method.
 *
 * @author Michael Nitschinger
 * @since 1.4.8
 */
@InterfaceAudience.Public
@InterfaceStability.Experimental
public interface CouchbaseCoreSendHook {

    Tuple2<CouchbaseRequest, Subject<CouchbaseResponse, CouchbaseResponse>>
        beforeSend(CouchbaseRequest originalRequest, Subject<CouchbaseResponse, CouchbaseResponse> originalResponse);
}
