/*
 * Copyright 2013 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.couchbase.client.deps.io.netty.handler.codec.memcache.binary;

import com.couchbase.client.core.endpoint.kv.MalformedMemcacheHeaderException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import static com.couchbase.client.core.endpoint.kv.KeyValueHandler.OP_APPEND;
import static com.couchbase.client.core.endpoint.kv.KeyValueHandler.OP_COUNTER_DECR;
import static com.couchbase.client.core.endpoint.kv.KeyValueHandler.OP_COUNTER_INCR;
import static com.couchbase.client.core.endpoint.kv.KeyValueHandler.OP_GET;
import static com.couchbase.client.core.endpoint.kv.KeyValueHandler.OP_GET_AND_LOCK;
import static com.couchbase.client.core.endpoint.kv.KeyValueHandler.OP_GET_AND_TOUCH;
import static com.couchbase.client.core.endpoint.kv.KeyValueHandler.OP_GET_REPLICA;
import static com.couchbase.client.core.endpoint.kv.KeyValueHandler.OP_INSERT;
import static com.couchbase.client.core.endpoint.kv.KeyValueHandler.OP_PREPEND;
import static com.couchbase.client.core.endpoint.kv.KeyValueHandler.OP_REPLACE;
import static com.couchbase.client.core.endpoint.kv.KeyValueHandler.OP_SUB_COUNTER;
import static com.couchbase.client.core.endpoint.kv.KeyValueHandler.OP_SUB_GET;
import static com.couchbase.client.core.endpoint.kv.KeyValueHandler.OP_UPSERT;


/**
 * The decoder which takes care of decoding the response headers.
 */
public class BinaryMemcacheResponseDecoder
    extends AbstractBinaryMemcacheDecoder<BinaryMemcacheResponse> {

    private static short MAX_KEY_LENGTH;

    private static int MAX_VALUE_LENGTH;

    static {
        /**
         * Max key length supported in Bytes
         */
        MAX_KEY_LENGTH = Short.parseShort(System.getProperty("com.couchbase.maxKVKeySize", "250"));

        /**
         * Max value length supported in MB
         */
        MAX_VALUE_LENGTH = Integer.parseInt(System.getProperty("com.couchbase.maxKVValueSize", "250")) * (int)Math.pow(10, 6);
    }

    public BinaryMemcacheResponseDecoder() {
        this(DEFAULT_MAX_CHUNK_SIZE);
    }

    public BinaryMemcacheResponseDecoder(int chunkSize) {
        super(chunkSize);
    }


    private boolean shouldHaveValue(byte opcode) {
        //Get key operations
        return (opcode == OP_GET ||
                opcode == OP_GET_AND_LOCK ||
                opcode == OP_GET_REPLICA ||
                opcode == OP_GET_AND_TOUCH ||
                opcode == OP_COUNTER_INCR ||
                opcode == OP_COUNTER_DECR ||
                opcode == OP_SUB_COUNTER ||
                opcode == OP_SUB_GET);

    }

    private boolean shouldHaveExtras(byte opcode) {
        return (opcode == OP_GET ||
                opcode == OP_GET_AND_LOCK ||
                opcode == OP_GET_REPLICA ||
                opcode == OP_GET_AND_TOUCH);
    }

    private boolean shouldHaveCAS(byte opcode) {
        //Most of the get and mutate operations
        return  (opcode == OP_GET ||
                opcode == OP_GET_AND_LOCK ||
                opcode == OP_GET_REPLICA ||
                opcode == OP_GET_AND_TOUCH ||
                opcode == OP_COUNTER_INCR ||
                opcode == OP_COUNTER_DECR ||
                opcode == OP_UPSERT ||
                opcode == OP_REPLACE ||
                opcode == OP_INSERT ||
                opcode == OP_APPEND ||
                opcode == OP_PREPEND ||
                ((opcode >= OP_SUB_GET) && (opcode <= OP_SUB_COUNTER)));
    }

    @Override
    protected BinaryMemcacheResponse decodeHeader(ByteBuf in) throws Exception {
        BinaryMemcacheResponse header = new DefaultBinaryMemcacheResponse();

        header.setMagic(in.readByte());
        header.setOpcode(in.readByte());
        header.setKeyLength(in.readShort());
        header.setExtrasLength(in.readByte());
        header.setDataType(in.readByte());
        header.setStatus(in.readShort());
        header.setTotalBodyLength(in.readInt());
        header.setOpaque(in.readInt());
        header.setCAS(in.readLong());

        byte opcode = header.getOpcode();
        byte magic = header.getMagic();
        short keyLength = header.getKeyLength();
        byte extrasLength = header.getExtrasLength();
        short status = header.getStatus();
        int bodyLength = header.getTotalBodyLength();
        long cas = header.getCAS();
        int maxBodyLength = MAX_VALUE_LENGTH + keyLength + extrasLength;
        boolean successful = (status == (short) 0x0);

        if (status < 0) {
            throw new MalformedMemcacheHeaderException("Invalid status, received " + status);
        }

        if (magic != DefaultBinaryMemcacheResponse.RESPONSE_MAGIC_BYTE) {
            throw new MalformedMemcacheHeaderException("Invalid response magic byte, received "
                    + magic + " bytes for opcode " + opcode);
        }

        if (keyLength < 0 || keyLength > MAX_KEY_LENGTH) {
            throw new MalformedMemcacheHeaderException("Invalid key length, received "
                    + header.getKeyLength() + " bytes for opcode " + opcode);
        }

        if ((successful && shouldHaveExtras(opcode) && extrasLength == 0) || extrasLength < 0) {
            throw new MalformedMemcacheHeaderException("Invalid extras length, received "
                    + extrasLength + "bytes for opcode " + opcode);
        }

        if ((successful && shouldHaveValue(opcode) && bodyLength == 0) || bodyLength < 0 || bodyLength > maxBodyLength) {
            throw new MalformedMemcacheHeaderException("Invalid response data length, received "
                    + bodyLength + " bytes for opcode " + opcode);
        }

        if (successful && shouldHaveCAS(opcode) && cas <= 0) {
            throw new MalformedMemcacheHeaderException("Invalid CAS value, received " + cas + " for opcode " + opcode);
        }

        return header;
    }

    @Override
    protected BinaryMemcacheResponse buildInvalidMessage() {
        return new DefaultBinaryMemcacheResponse(new byte[] {}, Unpooled.EMPTY_BUFFER);
    }
}
