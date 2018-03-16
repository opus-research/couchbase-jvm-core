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
package com.couchbase.client.core.endpoint.query;

/**
 * Verifies the correct functionality of the {@link QueryHandler} with V2 parser
 *
 * @author Subhashni Balakrishnan
 * @since 1.4.3
 */
public class QueryHandlerWithParserV2Test extends QueryHandlerWithParserV1Test {
    static {
        System.setProperty("com.couchbase.enableNewQueryResponseParser", "true");
    }
}
