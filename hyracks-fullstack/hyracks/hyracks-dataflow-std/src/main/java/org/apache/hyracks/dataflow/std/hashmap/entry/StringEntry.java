/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.dataflow.std.hashmap.entry;

import static org.apache.hyracks.unsafe.BytesToBytesMap.SEED;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.std.hashmap.Types;
import org.apache.hyracks.unsafe.BytesToBytesMap.Location;
import org.apache.hyracks.unsafe.entry.IEntry;
import org.apache.hyracks.util.encoding.VarLenIntEncoderDecoder;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.hash.Murmur3_x86_32;

public class StringEntry implements IEntry {
    private final ArrayBackedValueStorage storage;
    private IValueReference value;
    private int wastedSpace;

    public StringEntry() {
        storage = new ArrayBackedValueStorage();
    }

    public StringEntry(ArrayTupleBuilder tupleBuilder) {
        int[] fieldEndOffsets = tupleBuilder.getFieldEndOffsets();
        ByteBuffer byteBuffer = ByteBuffer.allocate(fieldEndOffsets.length * 4);

        // Put each integer into the ByteBuffer
        for (int i = 0; i < fieldEndOffsets.length; i++) {
            byteBuffer.putInt(fieldEndOffsets[i]);
        }
        byte[] fieldEndOffsetsBytes = byteBuffer.array();
        storage = new ArrayBackedValueStorage();
        int actualLength = tupleBuilder.getFieldData().getLength() + fieldEndOffsetsBytes.length;
        IValueReference newValue = new ArrayBackedValueStorage(tupleBuilder.getFieldData());
        if (actualLength % 8 != 0) {
            int newLength = ByteArrayMethods.roundNumberOfBytesToNearestWord(actualLength);
            storage.reset();
            storage.setSize(newLength);
            byte[] bytes = storage.getByteArray();
            System.arraycopy(fieldEndOffsetsBytes, 0, bytes, 0, fieldEndOffsetsBytes.length);
            System.arraycopy(newValue.getByteArray(), newValue.getStartOffset(), bytes, fieldEndOffsetsBytes.length,
                    actualLength - fieldEndOffsetsBytes.length);
            Arrays.fill(bytes, actualLength, newLength, (byte) 0);
        } else {
            storage.reset();
            storage.setSize(actualLength);
            byte[] bytes = storage.getByteArray();
            System.arraycopy(fieldEndOffsetsBytes, 0, bytes, 0, fieldEndOffsetsBytes.length);
            System.arraycopy(newValue.getByteArray(), newValue.getStartOffset(), bytes, fieldEndOffsetsBytes.length,
                    actualLength - fieldEndOffsetsBytes.length);
        }
        this.value = storage;
        wastedSpace = storage.getLength() - actualLength;
    }

    public void reset(IValueReference value) {
        if (value.getLength() % 8 == 0) {
            this.value = value;
        } else {
            storage.reset();
            int newLength = ByteArrayMethods.roundNumberOfBytesToNearestWord(value.getLength());
            storage.setSize(newLength);
            byte[] bytes = storage.getByteArray();
            System.arraycopy(value.getByteArray(), value.getStartOffset(), bytes, 0, value.getLength());
            Arrays.fill(bytes, value.getLength(), newLength, (byte) 0);

            this.value = storage;
            wastedSpace = storage.getLength() - value.getLength();
        }
    }

    public int getWastedSpace() {
        return wastedSpace;
    }

    @Override
    public boolean isEqual(Location location) {
        byte[] bytes = value.getByteArray();
        long valueOffset = value.getStartOffset() + (long) Platform.BYTE_ARRAY_OFFSET;
        int valueLength = value.getLength();

        Object baseObject = location.getKeyBase();
        long offset = location.getKeyOffset();
        int length = location.getKeyLength();
        return length == valueLength && ByteArrayMethods.arrayEquals(baseObject, offset, bytes, valueOffset, length);
    }

    @Override
    public void setValue(Location location) {
        set(location.getValueBase(), location.getValueOffset(), location.getValueLength());
    }

    @Override
    public void set(Object baseObject, long offset, long length) {
        long valueOffset = value.getStartOffset() + (long) Platform.BYTE_ARRAY_OFFSET;
        Platform.copyMemory(value.getByteArray(), valueOffset, baseObject, offset, length);
    }

    @Override
    public void get(Object baseObject, long offset, long length) {
        throw new IllegalStateException("Should not be called");
    }

    @Override
    public void getValue(Location location) {
        storage.setSize(location.getValueLength());
        byte[] bytes = storage.getByteArray();
        Platform.copyMemory(location.getValueBase(), location.getValueOffset(), bytes, Platform.BYTE_ARRAY_OFFSET,
                location.getValueLength());
        int originalLength = VarLenIntEncoderDecoder.decode(bytes, 0);
        int sizeByteLength = VarLenIntEncoderDecoder.getBytesRequired(originalLength);
        storage.setSize(sizeByteLength + originalLength);
        value = storage;
    }

    public IValueReference getValue() {
        return value;
    }

    @Override
    public int getLength() {
        return value.getLength();
    }

    @Override
    public int getHash() {
        byte[] bytes = value.getByteArray();
        long valueOffset = value.getStartOffset() + (long) Platform.BYTE_ARRAY_OFFSET;
        int valueLength = value.getLength();
        return Murmur3_x86_32.hashUnsafeWords(bytes, valueOffset, valueLength, SEED);
    }

    @Override
    public byte getEntryTypeOrdinal() {
        return Types.STRING.serialize();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        StringEntry that = (StringEntry) o;
        return UTF8StringPointable.areEqual(value, that.value);
    }

    @Override
    public int hashCode() {
        return getHash();
    }

    @Override
    public int compareTo(IEntry o) {
        if (!(o instanceof StringEntry)) {
            return getEntryTypeOrdinal() - o.getEntryTypeOrdinal();
        }
        return UTF8StringPointable.compare(value, ((StringEntry) o).getValue());
    }

    @Override
    public IEntry createCopy() {
        StringEntry copy = new StringEntry();

        copy.storage.set(value);
        copy.value = copy.storage;
        return copy;
    }

    @Override
    public void reset(IEntry other) {
        StringEntry otherString = (StringEntry) other;
        storage.set(otherString.value);
        value = storage;
    }

}
