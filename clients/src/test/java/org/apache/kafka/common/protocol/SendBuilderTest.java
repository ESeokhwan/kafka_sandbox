/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.protocol;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.FetchGlobalSequenceResponseData;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.UnalignedMemoryRecords;
import org.apache.kafka.common.requests.ByteBufferChannel;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SendBuilderTest {

    @Test
    public void testCoalescedGlobalFetchMatchesExistingFrame() throws IOException {
        MemoryRecords compressed = MemoryRecords.withRecords(Compression.gzip().build(), new SimpleRecord(Utils.utf8("first")));
        MemoryRecords plain = MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(Utils.utf8("second")));
        for (short version = 0; version <= 1; version++) {
            FetchGlobalSequenceResponseData response = new FetchGlobalSequenceResponseData()
                .setNextGlobalOffset(2)
                .setThrottleTimeMs(5)
                .setBatches(List.of(
                    new FetchGlobalSequenceResponseData.FetchedBatch().setPhysicalPartition(0).setRecords(compressed),
                    new FetchGlobalSequenceResponseData.FetchedBatch().setPhysicalPartition(1).setGlobalBaseOffset(1).setRecords(plain)));
            ResponseHeader header = new ResponseHeader(42, ApiKeys.FETCH_GLOBAL_SEQUENCE.responseHeaderVersion(version));
            for (Errors error : List.of(Errors.NONE, Errors.NOT_LEADER_OR_FOLLOWER)) {
                response.setErrorCode(error.code());
                ByteBuffer expected = TestUtils.toBuffer(SendBuilder.buildResponseSend(header, response, version));
                Send send = SendBuilder.buildCoalescedResponseSend(header, response, version, size -> {
                    assertEquals(expected.remaining(), size);
                    return true;
                });
                assertEquals(expected, readFrame(send, 1));
                Send fallback = SendBuilder.buildCoalescedResponseSend(header, response, version, size -> false);
                assertEquals(expected, readFrame(fallback, 5));
                assertEquals(0, compressed.buffer().position());
                assertEquals(0, plain.buffer().position());
            }
        }
    }

    private static ByteBuffer readFrame(Send send, int expectedBufferCount) throws IOException {
        try (ByteBufferChannel channel = new ByteBufferChannel(send.size()) {
            @Override
            public long write(ByteBuffer[] buffers) {
                assertEquals(expectedBufferCount, buffers.length);
                return super.write(buffers);
            }
        }) {
            send.writeTo(channel);
            return channel.buffer().duplicate().flip();
        }
    }

    @Test
    public void testZeroCopyByteBuffer() {
        byte[] data = Utils.utf8("foo");
        ByteBuffer zeroCopyBuffer = ByteBuffer.wrap(data);
        SendBuilder builder = new SendBuilder(8);

        builder.writeInt(5);
        builder.writeByteBuffer(zeroCopyBuffer);
        builder.writeInt(15);
        Send send = builder.build();

        // Overwrite the original buffer in order to prove the data was not copied
        byte[] overwrittenData = Utils.utf8("bar");
        assertEquals(data.length, overwrittenData.length);
        zeroCopyBuffer.rewind();
        zeroCopyBuffer.put(overwrittenData);
        zeroCopyBuffer.rewind();

        ByteBuffer buffer = TestUtils.toBuffer(send);
        assertEquals(8 + data.length, buffer.remaining());
        assertEquals(5, buffer.getInt());
        assertEquals("bar", getString(buffer, data.length));
        assertEquals(15, buffer.getInt());
    }

    @Test
    public void testWriteByteBufferRespectsPosition() {
        byte[] data = Utils.utf8("yolo");
        assertEquals(4, data.length);

        ByteBuffer buffer = ByteBuffer.wrap(data);
        SendBuilder builder = new SendBuilder(0);

        buffer.limit(2);
        builder.writeByteBuffer(buffer);
        assertEquals(0, buffer.position());

        buffer.position(2);
        buffer.limit(4);
        builder.writeByteBuffer(buffer);
        assertEquals(2, buffer.position());

        Send send = builder.build();
        ByteBuffer readBuffer = TestUtils.toBuffer(send);
        assertEquals("yolo", getString(readBuffer, 4));
    }

    @Test
    public void testZeroCopyRecords() {
        ByteBuffer buffer = ByteBuffer.allocate(128);
        MemoryRecords records = createRecords(buffer, "foo");

        SendBuilder builder = new SendBuilder(8);
        builder.writeInt(5);
        builder.writeRecords(records);
        builder.writeInt(15);
        Send send = builder.build();

        // Overwrite the original buffer in order to prove the data was not copied
        buffer.rewind();
        MemoryRecords overwrittenRecords = createRecords(buffer, "bar");

        ByteBuffer readBuffer = TestUtils.toBuffer(send);
        assertEquals(5, readBuffer.getInt());
        assertEquals(overwrittenRecords, getRecords(readBuffer, records.sizeInBytes()));
        assertEquals(15, readBuffer.getInt());
    }

    @Test
    public void testZeroCopyUnalignedRecords() {
        ByteBuffer buffer = ByteBuffer.allocate(128);
        MemoryRecords records = createRecords(buffer, "foo");

        ByteBuffer buffer1 = records.buffer().duplicate();
        buffer1.limit(buffer1.limit() / 2);

        ByteBuffer buffer2 = records.buffer().duplicate();
        buffer2.position(buffer2.limit() / 2);

        UnalignedMemoryRecords records1 = new UnalignedMemoryRecords(buffer1);
        UnalignedMemoryRecords records2 = new UnalignedMemoryRecords(buffer2);

        SendBuilder builder = new SendBuilder(8);
        builder.writeInt(5);
        builder.writeRecords(records1);
        builder.writeRecords(records2);
        builder.writeInt(15);
        Send send = builder.build();

        // Overwrite the original buffer in order to prove the data was not copied
        buffer.rewind();
        MemoryRecords overwrittenRecords = createRecords(buffer, "bar");

        ByteBuffer readBuffer = TestUtils.toBuffer(send);
        assertEquals(5, readBuffer.getInt());
        assertEquals(overwrittenRecords, getRecords(readBuffer, records.sizeInBytes()));
        assertEquals(15, readBuffer.getInt());
    }


    private String getString(ByteBuffer buffer, int size) {
        byte[] readData = new byte[size];
        buffer.get(readData);
        return Utils.utf8(readData);
    }

    private MemoryRecords getRecords(ByteBuffer buffer, int size) {
        int initialPosition = buffer.position();
        int initialLimit = buffer.limit();
        int recordsLimit = initialPosition + size;

        buffer.limit(recordsLimit);
        MemoryRecords records = MemoryRecords.readableRecords(buffer.slice());

        buffer.position(recordsLimit);
        buffer.limit(initialLimit);
        return records;
    }

    private MemoryRecords createRecords(ByteBuffer buffer, String value) {
        MemoryRecordsBuilder recordsBuilder = MemoryRecords.builder(
            buffer,
            Compression.NONE,
            TimestampType.CREATE_TIME,
            0L
        );
        recordsBuilder.append(new SimpleRecord(Utils.utf8(value)));
        return recordsBuilder.build();
    }

}
