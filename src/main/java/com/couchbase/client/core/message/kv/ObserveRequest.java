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
package com.couchbase.client.core.message.kv;

public class ObserveRequest extends AbstractKeyValueRequest {

    private final long cas;
    private final boolean master;
    private final short replica;

    public ObserveRequest(String key, long cas, boolean master, short replica, String bucket) {
        super(key, bucket);
        if (master && replica > 0) {
            throw new IllegalArgumentException("Either master or a replica node needs to be given");
        }
        this.cas = cas;
        this.master = master;
        this.replica = replica;
    }

    public long cas() {
        return cas;
    }

    public short replica() {
        return replica;
    }

    public boolean master() {
        return master;
    }
}
