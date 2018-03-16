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


/**
 * The decoder which takes care of decoding the response headers.
 */
public class BinaryMemcacheResponseDecoder
    extends AbstractBinaryMemcacheDecoder<BinaryMemcacheResponse> {

    /**
     * Max key length supported. Currently 250 bytes.
     */
    public static final short MAX_KEY_LENGTH = (short) 0xFA;

    /**
     * Max value length supported. Currently 20 megabytes.
     */
    public static final int MAX_VALUE_LENGTH = (int) 0x1FFFFF;

    public BinaryMemcacheResponseDecoder() {
        this(DEFAULT_MAX_CHUNK_SIZE);
    }

    public BinaryMemcacheResponseDecoder(int chunkSize) {
        super(chunkSize);
    }

    private boolean isKeyValueRequest(byte opcode) {
        //Set, get, add, replace, delete, incr, decr
        if (opcode >= (byte)0x00 && opcode <= (byte)0x06) {
            return true;
        }
        //append, prepend
        else if (opcode == 0x0e || opcode == 0x0f) {
            return true;
        }
        //subdocument operations
        else if (opcode >= (byte)0xc5 && opcode <= (byte)0xd2) {
            return true;
        }
        return false;
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

        if (status < 0) {
            throw new MalformedMemcacheHeaderException("Invalid status, received " + status);
        }

        //We need to check only the successful responses
        if (status == (short)0x0) {
            if (magic != DefaultBinaryMemcacheResponse.RESPONSE_MAGIC_BYTE) {
                throw new MalformedMemcacheHeaderException("Invalid response magic byte, received " + magic + " bytes");
            }

            if (isKeyValueRequest(opcode) && (keyLength) <= 0 || keyLength > MAX_KEY_LENGTH) {
                throw new MalformedMemcacheHeaderException("Invalid key length, received " + header.getKeyLength() + " bytes");
            }

            if (isKeyValueRequest(opcode) && extrasLength <= 0) {
                throw new MalformedMemcacheHeaderException("Invalid extras length, received " + extrasLength + "bytes");
            }

            if (isKeyValueRequest(opcode) && (bodyLength <= 0 || bodyLength > maxBodyLength)) {
                throw new MalformedMemcacheHeaderException("Invalid response data length, received " + bodyLength + " bytes");
            }

            if (isKeyValueRequest(opcode) && cas <= 0) {
                throw new MalformedMemcacheHeaderException("Invalid CAS value, received " + cas);
            }
        }
        return header;
    }

    @Override
    protected BinaryMemcacheResponse buildInvalidMessage() {
        return new DefaultBinaryMemcacheResponse(new byte[] {}, Unpooled.EMPTY_BUFFER);
    }
}
