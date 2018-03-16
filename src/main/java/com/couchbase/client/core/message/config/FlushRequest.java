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

import com.couchbase.client.core.message.AbstractCouchbaseRequest;

/**
 * Sends a flush command to the cluster.
 *
 * Note that depending on the return value, the executing client needs to perform more tasks like adding a document
 * and polling for its removal.
 *
 * @author Michael Nitschinger
 */
public class FlushRequest extends AbstractCouchbaseRequest implements ConfigRequest {

    private static final String PATH = "/controller/doFlush";

    public FlushRequest(String bucket, String password) {
        super(bucket, bucket, password);
    }

    public FlushRequest(String bucket, String username, String password) {
        super(bucket, username, password);
    }

    public String path() {
        return "/pools/default/buckets/" + bucket() + PATH;
    }
}
