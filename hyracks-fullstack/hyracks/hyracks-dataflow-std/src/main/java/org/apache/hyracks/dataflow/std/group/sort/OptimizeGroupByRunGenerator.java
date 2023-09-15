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

package org.apache.hyracks.dataflow.std.group.sort;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.io.GeneratedRunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.buffermanager.EnumFreeSlotPolicy;
import org.apache.hyracks.dataflow.std.buffermanager.FrameFreeSlotPolicyFactory;
import org.apache.hyracks.dataflow.std.buffermanager.IFrameBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.IFrameFreeSlotPolicy;
import org.apache.hyracks.dataflow.std.buffermanager.VariableFrameMemoryManager;
import org.apache.hyracks.dataflow.std.buffermanager.VariableFramePool;
import org.apache.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import org.apache.hyracks.dataflow.std.group.preclustered.OptimizeGroupWriter;
import org.apache.hyracks.dataflow.std.sort.FrameIterator;
import org.apache.hyracks.dataflow.std.sort.IFrameSorter;
import org.apache.hyracks.dataflow.std.sort.IRunGenerator;
import org.apache.hyracks.dataflow.std.sort.ISorter;

public class OptimizeGroupByRunGenerator implements IRunGenerator {

    private final int[] groupFields;
    private final IBinaryComparatorFactory[] comparatorFactories;
    private final IAggregatorDescriptorFactory aggregatorFactory;
    private final RecordDescriptor inRecordDesc;
    private final RecordDescriptor outRecordDesc;
    private final List<GeneratedRunFileReader> generatedRunFileReaders;

    protected final IHyracksTaskContext ctx;
    protected final IFrameSorter frameSorter;
    protected final int maxSortFrames;

    public OptimizeGroupByRunGenerator(IHyracksTaskContext ctx, int[] sortFields, RecordDescriptor inputRecordDesc,
            int framesLimit, int[] groupFields, INormalizedKeyComputerFactory[] keyNormalizerFactories,
            IBinaryComparatorFactory[] comparatorFactories, IAggregatorDescriptorFactory aggregatorFactory,
            RecordDescriptor outRecordDesc) throws HyracksDataException {

        this.groupFields = groupFields;
        this.comparatorFactories = comparatorFactories;
        this.aggregatorFactory = aggregatorFactory;
        this.inRecordDesc = inputRecordDesc;
        this.outRecordDesc = outRecordDesc;
        this.ctx = ctx;
        this.generatedRunFileReaders = new LinkedList<>();
        maxSortFrames = framesLimit - 1;

        IFrameFreeSlotPolicy freeSlotPolicy =
                FrameFreeSlotPolicyFactory.createFreeSlotPolicy(EnumFreeSlotPolicy.LAST_FIT, maxSortFrames);
        IFrameBufferManager bufferManager = new VariableFrameMemoryManager(
                new VariableFramePool(ctx, maxSortFrames * ctx.getInitialFrameSize()), freeSlotPolicy);
        frameSorter = new FrameIterator(ctx, bufferManager, maxSortFrames, sortFields, keyNormalizerFactories,
                comparatorFactories, inRecordDesc, Integer.MAX_VALUE);
    }

    @Override
    public void open() throws HyracksDataException {
        generatedRunFileReaders.clear();
    }

    @Override
    public void close() throws HyracksDataException {
        ISorter sorter = getSorter();
        if (sorter != null && sorter.hasRemaining()) {
            if (generatedRunFileReaders.size() <= 0) {
                sorter.sort();
            } else {
                flushFramesToRun();
            }
        }
    }

    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        if (!frameSorter.insertFrame(buffer)) {
            flushFramesToRun();
            if (!frameSorter.insertFrame(buffer)) {
                throw new HyracksDataException("The given frame is too big to insert into the sorting memory.");
            }
        }
    }

    protected RunFileWriter getRunFileWriter() throws HyracksDataException {
        FileReference file = ctx.getJobletContext()
                .createManagedWorkspaceFile(ExternalSortGroupByRunGenerator.class.getSimpleName());
        return new RunFileWriter(file, ctx.getIoManager());
    };

    protected IFrameWriter getFlushableFrameWriter(RunFileWriter writer) throws HyracksDataException {
        //create group-by comparators
        IBinaryComparator[] comparators =
                new IBinaryComparator[Math.min(groupFields.length, comparatorFactories.length)];
        for (int i = 0; i < comparators.length; i++) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }
        return new OptimizeGroupWriter(ctx, groupFields, comparators, aggregatorFactory, this.inRecordDesc,
                this.outRecordDesc, writer, true);
    }

    // assumption is that there will always be a sorter (i.e. sorter is not null)
    void flushFramesToRun() throws HyracksDataException {
        ISorter sorter = getSorter();
        sorter.sort();
        RunFileWriter runWriter = getRunFileWriter();
        IFrameWriter flushWriter = getFlushableFrameWriter(runWriter);
        flushWriter.open();
        try {
            sorter.flush(flushWriter);
        } catch (Exception e) {
            flushWriter.fail();
            throw e;
        } finally {
            flushWriter.close();
        }
        generatedRunFileReaders.add(runWriter.createDeleteOnCloseReader());
        sorter.reset();
    }

    public ISorter getSorter() {
        return frameSorter;
    }

    @Override
    public void fail() throws HyracksDataException {
    }

    @Override
    public List<GeneratedRunFileReader> getRuns() {
        return generatedRunFileReaders;
    }
}
