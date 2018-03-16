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

package com.couchbase.client.core.endpoint;

import com.couchbase.client.core.endpoint.kv.MalformedMemcacheHeaderException;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.BinaryMemcacheClientCodec;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.BinaryMemcacheResponse;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;

/**
 * Verifies if the codec handler behaves correctly on decoding failures
 */
public class BinaryMemcacheClientCodecHandlerTest {

    @Test
    public void shouldDetectMalformedHeaderAndSetDecoderResultAsMalformedMemcacheHeaderException() throws Exception{
        EmbeddedChannel channel = new EmbeddedChannel(new BinaryMemcacheClientCodec());

        ByteBuf byteBuf = Unpooled.buffer(24);
        //magic
        byteBuf.writeByte(0x81);
        //opcode
        byteBuf.writeByte(0x0);
        //key length
        byteBuf.writeShort(0x0);
        //extras length
        byteBuf.writeByte(0x04);
        //data type
        byteBuf.writeByte(0x0);
        //status
        byteBuf.writeShort(0x0);
        //body length
        byteBuf.writeInt(0x0);
        //opaque
        byteBuf.writeInt(0x0);
        //cas
        byteBuf.writeLong(0x0);


        channel.writeInbound(byteBuf);
        BinaryMemcacheResponse msg = (BinaryMemcacheResponse)channel.inboundMessages().peek();
        assert (msg.getDecoderResult().cause() instanceof MalformedMemcacheHeaderException);
    }
}
