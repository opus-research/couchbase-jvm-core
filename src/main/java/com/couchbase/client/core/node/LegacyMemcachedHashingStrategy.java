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
package com.couchbase.client.core.node;

import com.couchbase.client.core.config.NodeInfo;
import com.couchbase.client.core.service.ServiceType;

/**
 * The legacy memcache bucket hashing strategy, compatible with Java SDK 1.x.
 *
 * @author Michael Nitschinger
 * @since 2.3.6
 */
public class LegacyMemcachedHashingStrategy implements MemcachedHashingStrategy {

    public static LegacyMemcachedHashingStrategy INSTANCE = new LegacyMemcachedHashingStrategy();

    private LegacyMemcachedHashingStrategy() { }

    @Override
    public String hash(final NodeInfo info, final int repetition) {
        String hostname = info.hostname().address();
        if (hostname.startsWith("/")) {
            hostname = hostname.substring(1);
        }
        int port = info.services().get(ServiceType.BINARY);
        return hostname + ":" + port + "-" + repetition;
    }
}
