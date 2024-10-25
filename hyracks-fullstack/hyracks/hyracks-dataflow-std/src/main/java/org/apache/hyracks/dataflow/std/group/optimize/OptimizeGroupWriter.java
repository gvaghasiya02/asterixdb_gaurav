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
package org.apache.hyracks.dataflow.std.group.optimize;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.FloatPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.GrowableArray;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.group.AggregateState;
import org.apache.hyracks.dataflow.std.group.IAggregatorDescriptor;
import org.apache.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import org.apache.hyracks.dataflow.std.hashmap.AILRuntimeException;
import org.apache.hyracks.dataflow.std.hashmap.EnumDeserializeropt;
import org.apache.hyracks.dataflow.std.hashmap.Types;
import org.apache.hyracks.dataflow.std.hashmap.UnsafeAggregators;
import org.apache.hyracks.dataflow.std.hashmap.UnsafeComparators;
import org.apache.hyracks.dataflow.std.hashmap.UnsafeHashAggregator;
import org.apache.hyracks.dataflow.std.hashmap.entry.DoubleEntry;
import org.apache.hyracks.dataflow.std.hashmap.entry.LongEntry;
import org.apache.hyracks.dataflow.std.hashmap.entry.StringEntry;
import org.apache.hyracks.unsafe.BytesToBytesMap;
import org.apache.spark.unsafe.Platform;

public class OptimizeGroupWriter implements IFrameWriter {
    private final int[] groupFields;
    private final FrameTupleAccessor inFrameAccessor;
    private final ArrayTupleBuilder tupleBuilder;
    private final boolean groupAll;
    private boolean first;
    private boolean isFailed = false;
    private final long memoryLimit;
    private final AggregateState aggregateState;
    private FrameTupleAppender appender;
    private IFrameWriter writer;
    private UnsafeHashAggregator computer;
    private String aggregateType;
    private Types aggregateDataType; // datatype of field
    private final IAggregatorDescriptor aggregator;

    public OptimizeGroupWriter(IHyracksTaskContext ctx, int[] groupFields, RecordDescriptor inRecordDesc,
            RecordDescriptor outRecordDesc, IFrameWriter writer, boolean groupAll, int framesLimit,
            String aggregateType, IAggregatorDescriptorFactory aggregatorFactory) throws HyracksDataException {
        this.groupFields = groupFields;
        if (framesLimit >= 0 && framesLimit <= 3) {
            throw HyracksDataException.create(ErrorCode.ILLEGAL_MEMORY_BUDGET, "GROUP BY",
                    Long.toString(((long) (framesLimit)) * ctx.getInitialFrameSize()),
                    Long.toString(2L * ctx.getInitialFrameSize()));
        }

        this.memoryLimit = framesLimit <= 0 ? -1 : ((long) (framesLimit - 2)) * ctx.getInitialFrameSize();
        this.aggregateType = aggregateType;
        inFrameAccessor = new FrameTupleAccessor(inRecordDesc);
        VSizeFrame outFrame = new VSizeFrame(ctx);
        this.appender = new FrameTupleAppender();
        appender.reset(outFrame, true);
        this.writer = writer;
        tupleBuilder = new ArrayTupleBuilder(groupFields.length);
        this.groupAll = groupAll;
        this.aggregator = aggregatorFactory.createAggregator(ctx, inRecordDesc, outRecordDesc, groupFields, groupFields,
                writer, this.memoryLimit);
        this.aggregateState = aggregator.createAggregateStates();
    }

    @Override
    public void open() throws HyracksDataException {
        writer.open();
        first = true;
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        inFrameAccessor.reset(buffer);
        int nTuples = inFrameAccessor.getTupleCount();
        if (nTuples != 0) {
            for (int i = 0; i < nTuples; ++i) {
                boolean added;
                tupleBuilder.reset();
                for (int groupFieldIdx : groupFields) {
                    tupleBuilder.addField(inFrameAccessor, i, groupFieldIdx);
                }
                StringEntry st = new StringEntry(tupleBuilder);
                ArrayTupleBuilder tb = new ArrayTupleBuilder(1);
                aggregator.init(tupleBuilder, inFrameAccessor, i, aggregateState);

                aggregator.outputFinalResult(tb, null, 0, aggregateState);
                IValueReference newValue = new ArrayBackedValueStorage(tb.getFieldData());

                byte[] data = newValue.getByteArray();
                int offset = 0;

                Types typeTag = EnumDeserializeropt.ATYPETAGDESERIALIZER.deserialize(data[offset]);

                if (first) {
                    this.aggregateDataType = typeTag;
                    if (typeTag == Types.NULL || typeTag == Types.MISSING) {
                        continue;
                    }
                    if (typeTag == Types.TINYINT || typeTag == Types.SMALLINT || typeTag == Types.BIGINT
                            || typeTag == Types.INTEGER) {
                        computer = new UnsafeHashAggregator(UnsafeAggregators.getLongAggregator(aggregateType), null,
                                UnsafeComparators.STRING_COMPARATOR, memoryLimit);
                        LongEntry value = getLongEntryForTypeTag(typeTag, data, offset);
                        added = computer.aggregate(st, value);
                        if (!added) {
                            throw new HyracksDataException(
                                    "Key is too large for hash table use with complier.optimize.groupby set to false");
                        }
                    } else if (typeTag == Types.FLOAT || typeTag == Types.DOUBLE) {
                        computer = new UnsafeHashAggregator(UnsafeAggregators.getDoubleAggregator(aggregateType), null,
                                UnsafeComparators.STRING_COMPARATOR, memoryLimit);
                        DoubleEntry value = getDoubleEntryForTypeTag(typeTag, data, offset);
                        added = computer.aggregate(st, value);
                        if (!added) {
                            throw new HyracksDataException(
                                    "Key is too large for hash table use with complier.optimize.groupby set to false");
                        }
                    } else {
                        throw new AILRuntimeException("Aggregate type not supported " + typeTag.toString());
                    }
                    first = false;
                } else {
                    if (typeTag == Types.NULL || typeTag == Types.MISSING) {
                        continue;
                    }
                    if (aggregateDataType == Types.TINYINT || aggregateDataType == Types.SMALLINT
                            || aggregateDataType == Types.BIGINT || aggregateDataType == Types.INTEGER) {
                        LongEntry value = getLongEntryForTypeTag(typeTag, data, offset);
                        added = computer.aggregate(st, value);
                        if (!added) {
                            writeHashmap();
                            computer.reset();
                            added = computer.aggregate(st, value);
                        }
                    } else {
                        DoubleEntry value = getDoubleEntryForTypeTag(typeTag, data, offset);
                        added = computer.aggregate(st, value);
                        if (!added) {
                            writeHashmap();
                            computer.reset();
                            added = computer.aggregate(st, value);
                        }
                    }
                }
            }
        }
    }

    private LongEntry getLongEntryForTypeTag(Types typeTag, byte[] data, int offset) {
        LongEntry value = new LongEntry();
        if (typeTag == Types.TINYINT) {
            byte val = data[offset + 1];
            value.reset(val);
        } else if (typeTag == Types.SMALLINT) {
            short val = (short) (((data[offset + 1] & 0xff) << 8) + ((data[offset + 2] & 0xff) << 0));
            value.reset(val);
        } else if (typeTag == Types.INTEGER) {
            int val = IntegerPointable.getInteger(data, offset + 1);
            value.reset(val);
        } else {
            long val = LongPointable.getLong(data, offset + 1);
            value.reset(val);
        }
        return value;
    }

    private DoubleEntry getDoubleEntryForTypeTag(Types typeTag, byte[] data, int offset) {
        DoubleEntry value = new DoubleEntry();
        if (typeTag == Types.FLOAT) {
            float val = FloatPointable.getFloat(data, offset + 1);
            value.reset(val);
        } else {
            double val = DoublePointable.getDouble(data, offset + 1);
            value.reset(val);
        }
        return value;
    }

    private void writeHashmap() {
        try {
            if (!isFailed && (!first || groupAll)) {
                ArrayTupleBuilder tb = new ArrayTupleBuilder(groupFields.length + 1);
                DataOutput dos = tb.getDataOutput();
                Iterator<BytesToBytesMap.Location> iter = computer.aIterator();
                while (iter.hasNext()) {
                    BytesToBytesMap.Location location = iter.next();
                    tb.reset();
                    Object baseObject = location.getKeyBase();
                    long offset = location.getKeyOffset();
                    GrowableArray fieldArray = tb.getFieldData();
                    int fEndOffsetLength = this.groupFields.length * 4;
                    int writeOffset = fieldArray.getLength();
                    byte[] fEndOffsetBytes = new byte[fEndOffsetLength];
                    int unsafeOffset = writeOffset + Platform.BYTE_ARRAY_OFFSET;
                    Platform.copyMemory(baseObject, offset, fEndOffsetBytes, unsafeOffset, fEndOffsetLength);
                    tb.addAllFieldEndOffset(fEndOffsetBytes);
                    int actualLength = tb.getLastAddedOffset();
                    fieldArray.setSize(writeOffset + actualLength);
                    byte[] bytes = fieldArray.getByteArray();
                    Platform.copyMemory(baseObject, offset + fEndOffsetLength, bytes, unsafeOffset, actualLength);

                    if (aggregateDataType == Types.TINYINT || aggregateDataType == Types.SMALLINT
                            || aggregateDataType == Types.BIGINT || aggregateDataType == Types.INTEGER) {
                        long val = Platform.getLong(location.getValueBase(), location.getValueOffset());
                        try {
                            dos.writeByte(Types.BIGINT.serialize());
                            dos.writeLong(val);
                            tb.addFieldEndOffset();
                        } catch (IOException e) {
                            throw new AILRuntimeException("Hashmap cannot written");
                        }
                    } else {
                        double val = Platform.getDouble(location.getValueBase(), location.getValueOffset());
                        try {
                            dos.writeByte(Types.DOUBLE.serialize());
                            dos.writeDouble(val);
                            tb.addFieldEndOffset();
                        } catch (IOException e) {
                            throw new AILRuntimeException("Hashmap cannot written");
                        }
                    }
                    try {
                        if (tb.getSize() > 0) {
                            FrameUtils.appendSkipEmptyFieldToWriter(writer, appender, tb.getFieldEndOffsets(),
                                    tb.getByteArray(), 0, tb.getSize());
                        }
                    } catch (HyracksDataException e) {
                        throw new AILRuntimeException("Hashmap cannot write to buffer");
                    }
                }
            }
        } catch (Exception e) {
            throw e;
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        isFailed = true;
        writer.fail();
    }

    @Override
    public void close() throws HyracksDataException {
        try {
            writeHashmap();
            appender.write(writer, true);
        } catch (Exception e) {
            writer.fail();
            throw e;
        } finally {
            writer.close();
        }
    }
}
