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

package com.couchbase.client.core;

/**
 * Exception which states that the service is not available for the bucket.
 *
 * @author Michael Nitschinger
 * @since 2.0.3
 */
public class ServiceNotAvailableException extends CouchbaseException {

    public ServiceNotAvailableException() {
        super();
    }

    public ServiceNotAvailableException(String message) {
        super(message);
    }

    public ServiceNotAvailableException(String message, Throwable cause) {
        super(message, cause);
    }

    public ServiceNotAvailableException(Throwable cause) {
        super(cause);
    }
}
