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
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.FloatPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.GrowableArray;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import org.apache.hyracks.dataflow.std.wailhashmap.AILRuntimeException;
import org.apache.hyracks.dataflow.std.wailhashmap.EnumDeserializeropt;
import org.apache.hyracks.dataflow.std.wailhashmap.Types;
import org.apache.hyracks.dataflow.std.wailhashmap.UnsafeAggregators;
import org.apache.hyracks.dataflow.std.wailhashmap.UnsafeComparators;
import org.apache.hyracks.dataflow.std.wailhashmap.UnsafeHashAggregator;
import org.apache.hyracks.dataflow.std.wailhashmap.entry.LongEntry;
import org.apache.hyracks.dataflow.std.wailhashmap.entry.StringEntry;
import org.apache.hyracks.unsafe.BytesToBytesMap;
import org.apache.spark.unsafe.Platform;

public class OptimizeGroupWriter implements IFrameWriter {
    private final int[] groupFields;
    //        private final IBinaryComparator[] comparators;
    //        private final IAggregatorDescriptor aggregator;
    //        private final AggregateState aggregateState;
    private final FrameTupleAccessor inFrameAccessor;
    //    private final FrameTupleReference groupFieldsRef;
    //    private final PointableTupleReference groupFieldsPrevCopy;

    //    private final FrameTupleAppenderWrapper appenderWrapper;
    private final ArrayTupleBuilder tupleBuilder;
    private final boolean groupAll;
    //    private final boolean outputPartial;
    private boolean first;
    private boolean isFailed = false;
    private final long memoryLimit;
    private FrameTupleAppender appender;
    private IFrameWriter writer;
    private UnsafeHashAggregator computer;
    private RecordDescriptor outRecordDesc;

    private String aggregateType;
    //    private int counter;

    public OptimizeGroupWriter(IHyracksTaskContext ctx, int[] groupFields, IBinaryComparator[] comparators,
            IAggregatorDescriptorFactory aggregatorFactory, RecordDescriptor inRecordDesc,
            RecordDescriptor outRecordDesc, IFrameWriter writer, boolean outputPartial, boolean groupAll,
            int framesLimit, String aggregateType) throws HyracksDataException {
        this.groupFields = groupFields;
        //                this.comparators = comparators;

        if (framesLimit >= 0 && framesLimit <= 2) {
            throw HyracksDataException.create(ErrorCode.ILLEGAL_MEMORY_BUDGET, "GROUP BY",
                    Long.toString(((long) (framesLimit)) * ctx.getInitialFrameSize()),
                    Long.toString(2L * ctx.getInitialFrameSize()));
        }

        // Deducts input/output frames.
        this.memoryLimit = framesLimit <= 0 ? -1 : ((long) (framesLimit - 2)) * ctx.getInitialFrameSize();
        //                this.aggregator = aggregatorFactory.createAggregator(ctx, inRecordDesc, outRecordDesc, groupFields, groupFields,
        //                        writer, this.memoryLimit);
        //                this.aggregateState = aggregator.createAggregateStates();

        this.aggregateType = aggregateType;
        inFrameAccessor = new FrameTupleAccessor(inRecordDesc);
        //        groupFieldsRef = new PermutingFrameTupleReference(groupFields);
        //        groupFieldsPrevCopy = PointableTupleReference.create(groupFields.length, ArrayBackedValueStorage::new);
        VSizeFrame outFrame = new VSizeFrame(ctx);
        this.appender = new FrameTupleAppender();
        appender.reset(outFrame, true);
        this.writer = writer;
        //        appenderWrapper = new FrameTupleAppenderWrapper(appender, writer);
        tupleBuilder = new ArrayTupleBuilder(outRecordDesc.getFields().length);
        //        this.outputPartial = outputPartial;
        this.groupAll = groupAll;
        this.outRecordDesc = outRecordDesc;
    }

    @Override
    public void open() throws HyracksDataException {
        //        appenderWrapper.open();
        writer.open();
        //        counter = 0;
        first = true;
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        inFrameAccessor.reset(buffer);
        int nTuples = inFrameAccessor.getTupleCount();

        if (nTuples != 0) {
            for (int i = 0; i < nTuples; ++i) {
                tupleBuilder.reset();
                for (int groupFieldIdx : groupFields) {
                    tupleBuilder.addField(inFrameAccessor, i, groupFieldIdx);
                }
                StringEntry st = new StringEntry(tupleBuilder.getFieldData());
                LongEntry value = new LongEntry();
                //                if (first) {
                if (aggregateType == "COUNT") {
                    value.reset(1);

                    first = false;
                    computer.aggregate(st, value);
                } else {
//                    computer = new UnsafeHashAggregator(UnsafeAggregators.getLongAggregator(aggregateType), null,
//                            UnsafeComparators.STRING_COMPARATOR, memoryLimit);
                    FrameTupleReference ftr = new FrameTupleReference();
                    ftr.reset(inFrameAccessor, i);

                    //                    IEvaluatorContext evalCtx = new EvaluatorContext(this.ctx);
                    IPointable res = new VoidPointable();
                    byte[] bfr = ftr.getFieldData(0);
                    int start = ftr.getFieldStart(0);
                    int length = ftr.getFieldLength(0);
                    res.set(bfr, start, length);
                    byte[] data = res.getByteArray();
                    int offset = res.getStartOffset();

                    Types aggType = Types.SYSTEM_NULL;
                    // Get the data type tag
                    Types typeTag = EnumDeserializeropt.ATYPETAGDESERIALIZER.deserialize(data[offset]);
                    //
                    //                    // Handle MISSING and NULL values
                    if (typeTag == Types.MISSING || typeTag == Types.NULL) {
                        return;
                    }
                    // Non-missing and Non-null
                    else if (aggType == Types.SYSTEM_NULL) {
                        aggType = typeTag;
                    }

                    // Calculate based on the incoming data type + handles invalid data type
                    switch (typeTag) {
                        case TINYINT: {
                            byte val = data[offset + 1];
                            value.reset(val);
                            break;
                        }
                        case SMALLINT: {
                            short val = (short) (((data[offset + 1] & 0xff) << 8) + ((data[offset + 2] & 0xff) << 0));
                            value.reset(val);
                            break;
                        }
                        case INTEGER: {
                            int val = IntegerPointable.getInteger(data, offset + 1);
                            value.reset(val);
                            break;
                        }
                        case BIGINT: {
                            long val = LongPointable.getLong(data, offset + 1);
                            value.reset(val);
                            break;
                        }
                        case FLOAT: {
                            float val = FloatPointable.getFloat(data, offset + 1);

                            break;
                        }
                        case DOUBLE: {
                            double val = DoublePointable.getDouble(data, offset + 1);
                            break;
                        }
                        case SYSTEM_NULL: {
                            break;
                        }
                        default: {
                            // Issue warning only once and treat current tuple as null

                        }
                    }
                    computer.aggregate(st, value);
                }
            }

            //                                else {
            //
            //                    if (i == 0) {
            //                        switchGroupIfRequired(groupFieldsPrevCopy, inFrameAccessor, 0);
            //                    } else {
            //                        groupFieldsRef.reset(inFrameAccessor, i - 1);
            //                        switchGroupIfRequired(groupFieldsRef, inFrameAccessor, i);
            //                    }
            //                }
        }
        //            groupFieldsRef.reset(inFrameAccessor, nTuples - 1);
        //            groupFieldsPrevCopy.set(groupFieldsRef);
    }
    //    }

    //    private void switchGroupIfRequired(ITupleReference prevTupleGroupFields, IFrameTupleAccessor currTupleAccessor,
    //            int currTupleIndex) throws HyracksDataException {
    //        if (!sameGroup(prevTupleGroupFields, currTupleAccessor, currTupleIndex, groupFields, comparators)) {
    //            writeOutput(prevTupleGroupFields);
    //            tupleBuilder.reset();
    //            for (int groupFieldIdx : groupFields) {
    //                tupleBuilder.addField(currTupleAccessor, currTupleIndex, groupFieldIdx);
    //            }
    //            aggregator.init(tupleBuilder, currTupleAccessor, currTupleIndex, aggregateState);
    //        } else {
    //            aggregator.aggregate(currTupleAccessor, currTupleIndex, null, 0, aggregateState);
    //        }
    //    }

        private void writeHashmap() {
            int ss = computer.size();
            ArrayTupleBuilder tb = new ArrayTupleBuilder(outRecordDesc.getFields().length);
            DataOutput dos = tb.getDataOutput();
            Iterator<BytesToBytesMap.Location> iter = computer.sortedIterator();
            //            StringEntry key = new StringEntry();
            while (iter.hasNext()) {
                BytesToBytesMap.Location location = iter.next();
                tb.reset();
                Object baseObject = location.getKeyBase();
                long offset = location.getKeyOffset();
                long alignedLength = location.getKeyLength();
                //                    int encodedLength = StringEntryUtil.decode(baseObject, offset, alignedLength);
                //                    int actualLength = encodedLength + VarLenIntEncoderDecoder.getBytesRequired(encodedLength);

                GrowableArray fieldArray = tb.getFieldData();
                //                int typeTagOffset = fieldArray.getLength();
                int writeOffset = fieldArray.getLength();
                fieldArray.setSize((int) (writeOffset + alignedLength));

                byte[] bytes = fieldArray.getByteArray();
                int unsafeOffset = writeOffset + Platform.BYTE_ARRAY_OFFSET;
                //                bytes[typeTagOffset] = Types.STRING.serialize();
                Platform.copyMemory(baseObject, offset, bytes, unsafeOffset, alignedLength);
                tb.addFieldEndOffset();
                long val = Platform.getLong(location.getValueBase(), location.getValueOffset());
                try {
                    dos.writeByte(Types.BIGINT.serialize());
                    dos.writeLong(val);
                    tb.addFieldEndOffset();
                } catch (IOException e) {
                    throw new AILRuntimeException();
                }
                try {
                    if (tb.getSize() > 0) {
                        FrameUtils.appendSkipEmptyFieldToWriter(writer, appender, tb.getFieldEndOffsets(),
                                tb.getByteArray(), 0, tb.getSize());
                    }
                } catch (HyracksDataException e) {
                    throw new AILRuntimeException();
                }
            }
        }
    //    private void writeOutput(ITupleReference lastTupleGroupFields) throws HyracksDataException {
    //        tupleBuilder.reset();
    //        for (int i = 0; i < groupFields.length; i++) {
    //            tupleBuilder.addField(lastTupleGroupFields, i);
    //        }
    //        boolean hasOutput = outputPartial ? aggregator.outputPartialResult(tupleBuilder, null, 0, aggregateState)
    //                : aggregator.outputFinalResult(tupleBuilder, null, 0, aggregateState);
    //        if (hasOutput) {
    //            appenderWrapper.appendSkipEmptyField(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
    //                    tupleBuilder.getSize());
    //        }
    //    }

    //    public static boolean sameGroup(ITupleReference prevTupleGroupFields, IFrameTupleAccessor curTupleAccessor,
    //            int curTupleIdx, int[] curTupleGroupFields, IBinaryComparator[] comparators) throws HyracksDataException {
    //        for (int i = 0; i < comparators.length; ++i) {
    //            byte[] prevTupleFieldData = prevTupleGroupFields.getFieldData(i);
    //            int prevTupleFieldStart = prevTupleGroupFields.getFieldStart(i);
    //            int prevTupleFieldLength = prevTupleGroupFields.getFieldLength(i);
    //
    //            byte[] curTupleFieldData = curTupleAccessor.getBuffer().array();
    //            int curTupleFieldIdx = curTupleGroupFields[i];
    //            int curTupleFieldStart = curTupleAccessor.getAbsoluteFieldStartOffset(curTupleIdx, curTupleFieldIdx);
    //            int curTupleFieldLength = curTupleAccessor.getFieldLength(curTupleIdx, curTupleFieldIdx);
    //
    //            if (comparators[i].compare(prevTupleFieldData, prevTupleFieldStart, prevTupleFieldLength, curTupleFieldData,
    //                    curTupleFieldStart, curTupleFieldLength) != 0) {
    //                return false;
    //            }
    //        }
    //        return true;
    //    }

    @Override
    public void fail() throws HyracksDataException {
        isFailed = true;
        writer.fail();
        //        appenderWrapper.fail();
    }

    @Override
    public void close() throws HyracksDataException {
        try {
            //                            writeOutput(groupFieldsPrevCopy);
//            int ss = computer.size();
//            ArrayTupleBuilder tb = new ArrayTupleBuilder(outRecordDesc.getFields().length);
//            DataOutput dos = tb.getDataOutput();
//            Iterator<BytesToBytesMap.Location> iter = computer.sortedIterator();
//            //            StringEntry key = new StringEntry();
//            while (iter.hasNext()) {
//                BytesToBytesMap.Location location = iter.next();
//                tb.reset();
//                Object baseObject = location.getKeyBase();
//                long offset = location.getKeyOffset();
//                long alignedLength = location.getKeyLength();
//                //                    int encodedLength = StringEntryUtil.decode(baseObject, offset, alignedLength);
//                //                    int actualLength = encodedLength + VarLenIntEncoderDecoder.getBytesRequired(encodedLength);
//
//                GrowableArray fieldArray = tb.getFieldData();
//                //                int typeTagOffset = fieldArray.getLength();
//                int writeOffset = fieldArray.getLength();
//                fieldArray.setSize((int) (writeOffset + alignedLength));
//
//                byte[] bytes = fieldArray.getByteArray();
//                int unsafeOffset = writeOffset + Platform.BYTE_ARRAY_OFFSET;
//                //                bytes[typeTagOffset] = Types.STRING.serialize();
//                Platform.copyMemory(baseObject, offset, bytes, unsafeOffset, alignedLength);
//                tb.addFieldEndOffset();
//                long val = Platform.getLong(location.getValueBase(), location.getValueOffset());
//                try {
//                    dos.writeByte(Types.BIGINT.serialize());
//                    dos.writeLong(val);
//                    tb.addFieldEndOffset();
//                } catch (IOException e) {
//                    throw new AILRuntimeException();
//                }
//                try {
//                    if (tb.getSize() > 0) {
//                        FrameUtils.appendSkipEmptyFieldToWriter(writer, appender, tb.getFieldEndOffsets(),
//                                tb.getByteArray(), 0, tb.getSize());
//                    }
//                } catch (HyracksDataException e) {
//                    throw new AILRuntimeException();
//                }
//            }
            writeHashmap();
            appender.write(writer, true);
            //            aggregator.close();
            //            aggregateState.close();
        } catch (Exception e) {
            writer.fail();
            throw e;
        } finally {
            writer.close();
        }
    }
}
