/*
 * Copyright (c) 2015 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */
package com.couchbase.client.core.cluster;

import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.kv.GetRequest;
import com.couchbase.client.core.message.kv.GetResponse;
import com.couchbase.client.core.message.kv.MutationToken;
import com.couchbase.client.core.message.kv.RemoveRequest;
import com.couchbase.client.core.message.kv.RemoveResponse;
import com.couchbase.client.core.message.kv.UpsertRequest;
import com.couchbase.client.core.message.kv.UpsertResponse;
import com.couchbase.client.core.message.kv.subdoc.SimpleSubdocResponse;
import com.couchbase.client.core.message.kv.subdoc.SubDictAddRequest;
import com.couchbase.client.core.message.kv.subdoc.SubExistRequest;
import com.couchbase.client.core.message.kv.subdoc.SubGetRequest;
import com.couchbase.client.core.message.kv.subdoc.SubDictUpsertRequest;
import com.couchbase.client.core.util.ClusterDependentTest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ResourceLeakDetector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Verifies basic functionality of binary subdocument operations.
 *
 * @author Simon Baslé
 * @since 1.2
 */
public class SubdocumentMessageTest extends ClusterDependentTest {

    static {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

    /** Use this key when you want previous data to exist in your test*/
    private static final String testSubKey = "testSubKey";

    /** Use this key when you want to create data in your test, to be cleaned up after*/
    private static final String testInsertionSubKey = "testInsertionSubKey";

    private static final String jsonContent = "{\"value\":\"stringValue\", \"sub\": {\"value\": \"subStringValue\",\"array\": [\"array1\", 2, true]}}";

    @Before
    public void prepareData() {
        UpsertRequest upsert = new UpsertRequest(testSubKey, Unpooled.copiedBuffer(jsonContent, CharsetUtil.UTF_8), bucket(), true);
        UpsertResponse response = cluster().<UpsertResponse>send(upsert).toBlocking().single();
        ReferenceCountUtil.releaseLater(response.content());
        assertTrue(response.status().isSuccess());
    }

    @After
    public void deleteData() {
        RemoveRequest remove = new RemoveRequest(testSubKey, bucket());
        RemoveResponse response = cluster().<RemoveResponse>send(remove).toBlocking().single();
        boolean removedSubkey = response.status().isSuccess();
        ReferenceCountUtil.releaseLater(response.content());

        remove = new RemoveRequest(testInsertionSubKey, bucket());
        response = cluster().<RemoveResponse>send(remove).toBlocking().single();
        boolean removedInsertionSubkey = response.status().isSuccess() || response.status().equals(ResponseStatus.NOT_EXISTS);
        ReferenceCountUtil.releaseLater(response.content());

        assertTrue("Couldn't remove " + testSubKey, removedSubkey);
        assertTrue("Couldn't remove " + testInsertionSubKey, removedInsertionSubkey);
    }

    @Test
    public void shouldSubGetValueInExistingDocumentObject() throws Exception{
        String subValuePath = "sub.value";

        SubGetRequest valueRequest = new SubGetRequest(testSubKey, subValuePath, bucket());
        SimpleSubdocResponse valueResponse = cluster().<SimpleSubdocResponse>send(valueRequest).toBlocking().single();
        String raw = valueResponse.content().toString(CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(valueResponse.content());
        assertNotNull(raw);
        assertEquals("\"subStringValue\"", raw);
    }

    @Test
    public void shouldSubGetValueInExistingDocumentArray() throws Exception{
        String subArrayPath = "sub.array[1]";

        SubGetRequest arrayRequest = new SubGetRequest(testSubKey, subArrayPath, bucket());
        SimpleSubdocResponse arrayResponse = cluster().<SimpleSubdocResponse>send(arrayRequest).toBlocking().single();
        String raw = arrayResponse.content().toString(CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(arrayResponse.content());
        assertNotNull(raw);
        assertEquals("2", raw);
    }

    @Test
    public void shouldFailSubGetIfPathDoesntExist() {
        String wrongPath = "sub.arra[1]";

        SubGetRequest badRequest = new SubGetRequest(testSubKey, wrongPath, bucket());
        SimpleSubdocResponse response = cluster().<SimpleSubdocResponse>send(badRequest).toBlocking().single();

        assertFalse(response.status().isSuccess());
        assertEquals(ResponseStatus.SUBDOC_PATH_NOT_FOUND, response.status());
    }

    @Test
    public void shouldSucceedExistOnDictionnaryPath() {
        String path = "sub.value";

        SubExistRequest request = new SubExistRequest(testSubKey, path, bucket());
        SimpleSubdocResponse response = cluster().<SimpleSubdocResponse>send(request).toBlocking().single();

        assertEquals(ResponseStatus.SUCCESS, response.status());
    }

    @Test
    public void shouldSucceedExistOnArrayPath() {
        String path = "sub.array[1]";

        SubExistRequest request = new SubExistRequest(testSubKey, path, bucket());
        SimpleSubdocResponse response = cluster().<SimpleSubdocResponse>send(request).toBlocking().single();

        assertEquals(ResponseStatus.SUCCESS, response.status());
    }

    @Test
    public void shouldReturnPathNotFoundOnExistWithBadPath() {
        String path = "sub.bad";

        SubExistRequest request = new SubExistRequest(testSubKey, path, bucket());
        SimpleSubdocResponse response = cluster().<SimpleSubdocResponse>send(request).toBlocking().single();

        assertEquals(ResponseStatus.SUBDOC_PATH_NOT_FOUND, response.status());;
    }

    @Test
    public void testExistDoesntContainValueOnSuccess() {
        String path = "sub.array[1]";

        SubExistRequest request = new SubExistRequest(testSubKey, path, bucket());
        SimpleSubdocResponse response = cluster().<SimpleSubdocResponse>send(request).toBlocking().single();

        assertEquals(ResponseStatus.SUCCESS, response.status());
        assertEquals(0, response.content().readableBytes());
    }

    @Test
    public void shouldDictUpsertInExistingDocumentObject() {
        String subPath = "sub.value";
        ByteBuf fragment = Unpooled.copiedBuffer("\"mutated\"", CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(fragment);

        //verify initial state
        GetResponse initialState = cluster().<GetResponse>send(new GetRequest(testSubKey, bucket())).toBlocking().single();
        ReferenceCountUtil.releaseLater(initialState.content());
        assertEquals(jsonContent, initialState.content().toString(CharsetUtil.UTF_8));

        //mutate
        SubDictUpsertRequest upsertRequest = new SubDictUpsertRequest(testSubKey, subPath, fragment, bucket());
        SimpleSubdocResponse upsertResponse = cluster().<SimpleSubdocResponse>send(upsertRequest).toBlocking().single();
        ReferenceCountUtil.releaseLater(upsertResponse.content());
        assertTrue(upsertResponse.status().isSuccess());

        //verify mutated state
        GetResponse finalState = cluster().<GetResponse>send(new GetRequest(testSubKey, bucket())).toBlocking().single();
        ReferenceCountUtil.releaseLater(finalState.content());
        assertEquals(jsonContent.replace("subStringValue", "mutated"), finalState.content().toString(CharsetUtil.UTF_8));
    }

    @Test
    public void shouldReturnPathInvalidOnDictUpsertInArray() {
        String subPath = "sub.array[1]";
        ByteBuf fragment = Unpooled.copiedBuffer("\"mutated\"", CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(fragment);

        //mutate
        SubDictUpsertRequest upsertRequest = new SubDictUpsertRequest(testSubKey, subPath, fragment, bucket());
        SimpleSubdocResponse upsertResponse = cluster().<SimpleSubdocResponse>send(upsertRequest).toBlocking().single();
        ReferenceCountUtil.releaseLater(upsertResponse.content());
        assertFalse(upsertResponse.status().isSuccess());
        assertEquals(0, upsertResponse.content().readableBytes());
        assertEquals(ResponseStatus.SUBDOC_PATH_INVALID, upsertResponse.status());
    }

    @Test
    public void shouldGetMutationTokenWithMutation() throws Exception {
        String subPath = "sub.value";
        ByteBuf fragment = Unpooled.copiedBuffer("\"mutated\"", CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(fragment);

        SubDictUpsertRequest upsertRequest = new SubDictUpsertRequest(testSubKey, subPath, fragment, bucket());
        SimpleSubdocResponse upsertResponse = cluster().<SimpleSubdocResponse>send(upsertRequest).toBlocking().single();
        ReferenceCountUtil.releaseLater(upsertResponse.content());
        assertValidMetadata(upsertResponse.mutationToken());
    }

    @Test
    public void shouldNotGetMutationTokenWithGet() throws Exception {
        String subPath = "sub.value";
        ByteBuf fragment = Unpooled.copiedBuffer("\"mutated\"", CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(fragment);

        SubGetRequest getRequest = new SubGetRequest(testSubKey, subPath, bucket());
        SimpleSubdocResponse response = cluster().<SimpleSubdocResponse>send(getRequest).toBlocking().single();
        ReferenceCountUtil.releaseLater(response.content());
        assertNull(response.mutationToken());
    }

    @Test
    public void shouldDictAddOnSubObject() {
        String subPath = "sub.otherValue";
        ByteBuf fragment = Unpooled.copiedBuffer("\"inserted\"", CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(fragment);

        //mutate
        SubDictAddRequest insertRequest = new SubDictAddRequest(testSubKey, subPath, fragment, bucket());
        SimpleSubdocResponse insertResponse = cluster().<SimpleSubdocResponse>send(insertRequest).toBlocking().single();
        ReferenceCountUtil.releaseLater(insertResponse.content());
        assertTrue(insertResponse.status().isSuccess());
        assertEquals(0, insertResponse.content().readableBytes());

        //check the insertion at the end of "sub" object
        String expected = "{\"value\":\"stringValue\", \"sub\": {\"value\": \"subStringValue\",\"array\": [\"array1\", 2, true]" +
                ",\"otherValue\":\"inserted\"}}";

        GetResponse finalState = cluster().<GetResponse>send(new GetRequest(testSubKey, bucket())).toBlocking().single();
        ReferenceCountUtil.releaseLater(finalState.content());
        assertEquals(expected, finalState.content().toString(CharsetUtil.UTF_8));
    }

    @Test
    public void shouldReturnPathExistOnDictAddOnSubValue() {
        String subPath = "sub.value";
        ByteBuf fragment = Unpooled.copiedBuffer("\"mutated\"", CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(fragment);

        //mutate
        SubDictAddRequest insertRequest = new SubDictAddRequest(testSubKey, subPath, fragment, bucket());
        SimpleSubdocResponse insertResponse = cluster().<SimpleSubdocResponse>send(insertRequest).toBlocking().single();
        ReferenceCountUtil.releaseLater(insertResponse.content());
        assertFalse(insertResponse.status().isSuccess());
        assertEquals(0, insertResponse.content().readableBytes());
        assertEquals(ResponseStatus.SUBDOC_PATH_EXISTS, insertResponse.status());
    }

    @Test
    public void shouldReturnPathNotFoundOnDictAddForNewDeepPath() {
        String subPath = "sub2.value";
        ByteBuf fragment = Unpooled.copiedBuffer("\"insertedPath\"", CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(fragment);

        //mutate
        SubDictAddRequest insertRequest = new SubDictAddRequest(testSubKey, subPath, fragment, bucket());
        assertFalse(insertRequest.createIntermediaryPath());

        SimpleSubdocResponse insertResponse = cluster().<SimpleSubdocResponse>send(insertRequest).toBlocking().single();
        ReferenceCountUtil.releaseLater(insertResponse.content());
        assertFalse(insertResponse.status().isSuccess());
        assertEquals(0, insertResponse.content().readableBytes());
        assertEquals(ResponseStatus.SUBDOC_PATH_NOT_FOUND, insertResponse.status());
    }

    @Test
    public void shouldReturnPathInvalidOnDictAddForArrayPath() {
        String subPath = "sub.array[1]";
        ByteBuf fragment = Unpooled.copiedBuffer("\"insertedPath\"", CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(fragment);

        //mutate
        SubDictAddRequest insertRequest = new SubDictAddRequest(testSubKey, subPath, fragment, bucket());
        assertFalse(insertRequest.createIntermediaryPath());

        SimpleSubdocResponse insertResponse = cluster().<SimpleSubdocResponse>send(insertRequest).toBlocking().single();
        ReferenceCountUtil.releaseLater(insertResponse.content());
        assertFalse(insertResponse.status().isSuccess());
        assertEquals(0, insertResponse.content().readableBytes());
        assertEquals(ResponseStatus.SUBDOC_PATH_INVALID, insertResponse.status());
    }

    @Test
    public void shouldCreateIntermediaryNodesOnDictAddForNewDeepPathIfForced() {
        String subPath = "sub2.value";
        ByteBuf fragment = Unpooled.copiedBuffer("\"insertedPath\"", CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(fragment);

        //mutate
        SubDictAddRequest insertRequest = new SubDictAddRequest(testSubKey, subPath, fragment, bucket());
        insertRequest.createIntermediaryPath(true);

        SimpleSubdocResponse insertResponse = cluster().<SimpleSubdocResponse>send(insertRequest).toBlocking().single();
        ReferenceCountUtil.releaseLater(insertResponse.content());
        assertTrue(insertResponse.status().isSuccess());
        assertEquals(0, insertResponse.content().readableBytes());
    }

    @Test
    public void shouldNotCreateIntermediaryNodesOnDictAddForNewDeepPathByDefault() {
        String subPath = "sub2.value";
        ByteBuf fragment = Unpooled.copiedBuffer("\"insertedPath\"", CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(fragment);

        SubDictAddRequest insertRequest = new SubDictAddRequest(testSubKey, subPath, fragment, bucket());
        SimpleSubdocResponse insertResponse = cluster().<SimpleSubdocResponse>send(insertRequest).toBlocking().single();
        ReferenceCountUtil.releaseLater(insertResponse.content());

        assertFalse(insertResponse.status().isSuccess());
        assertEquals(0, insertResponse.content().readableBytes());
        assertEquals(ResponseStatus.SUBDOC_PATH_NOT_FOUND, insertResponse.status());
    }

    /**
     * Helper method to assert if the mutation metadata is correct.
     *
     * Note that if mutation metadata is disabled, null is expected.
     *
     * @param token the token to check
     * @throws Exception
     */
    private void assertValidMetadata(MutationToken token) throws Exception {
        if (isMutationMetadataEnabled()) {
            assertNotNull(token);
            assertTrue(token.sequenceNumber() > 0);
            assertTrue(token.vbucketUUID() != 0);
            assertTrue(token.vbucketID() > 0);
        } else {
            assertNull(token);
        }
    }
}
