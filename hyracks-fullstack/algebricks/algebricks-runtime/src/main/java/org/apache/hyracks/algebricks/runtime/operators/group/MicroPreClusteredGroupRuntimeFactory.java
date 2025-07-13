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
package org.apache.hyracks.algebricks.runtime.operators.group;

import java.nio.ByteBuffer;

import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputPushRuntime;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputRuntimeFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.dataflow.std.buffermanager.CBOMemoryBudget;
import org.apache.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import org.apache.hyracks.dataflow.std.group.preclustered.PreclusteredGroupWriter;

public class MicroPreClusteredGroupRuntimeFactory extends AbstractOneInputOneOutputRuntimeFactory {

    private static final long serialVersionUID = 1L;
    private final int[] groupFields;
    private final IBinaryComparatorFactory[] comparatorFactories;
    private final IAggregatorDescriptorFactory aggregatorFactory;
    private final RecordDescriptor inRecordDesc;
    private final RecordDescriptor outRecordDesc;
    private final CBOMemoryBudget cboMemoryBudget;
    private int framesLimit;

    public MicroPreClusteredGroupRuntimeFactory(int[] groupFields, IBinaryComparatorFactory[] comparatorFactories,
            IAggregatorDescriptorFactory aggregatorFactory, RecordDescriptor inRecordDesc,
            RecordDescriptor outRecordDesc, int[] projectionList, CBOMemoryBudget cboMemoryBudget) {
        super(projectionList);
        // Obs: the projection list is currently ignored.
        if (projectionList != null) {
            throw new NotImplementedException("Cannot push projection into InMemorySortRuntime.");
        }
        this.groupFields = groupFields;
        this.comparatorFactories = comparatorFactories;
        this.aggregatorFactory = aggregatorFactory;
        this.inRecordDesc = inRecordDesc;
        this.outRecordDesc = outRecordDesc;
        this.cboMemoryBudget = cboMemoryBudget;
        this.framesLimit = cboMemoryBudget.sizeInFrames();
    }

    @Override
    public AbstractOneInputOneOutputPushRuntime createOneOutputPushRuntime(final IHyracksTaskContext ctx)
            throws HyracksDataException {
        if (ctx.getJobFlags().contains(JobFlag.USE_CBO_MAX_MEMORY) && cboMemoryBudget.cboMaxSizeInFrames() > 0) {
            framesLimit = cboMemoryBudget.cboMaxSizeInFrames();
        }
        if (ctx.getJobFlags().contains(JobFlag.USE_CBO_OPTIMAL_MEMORY)
                && cboMemoryBudget.cboOptimalSizeInFrames() > 0) {
            framesLimit = cboMemoryBudget.cboOptimalSizeInFrames();
        }
        final IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; ++i) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }

        return new AbstractOneInputOneOutputPushRuntime() {

            private PreclusteredGroupWriter pgw;

            @Override
            public void open() throws HyracksDataException {
                pgw = new PreclusteredGroupWriter(ctx, groupFields, comparators, aggregatorFactory, inRecordDesc,
                        outRecordDesc, writer, false, false, framesLimit);
                pgw.open();
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                pgw.nextFrame(buffer);
            }

            @Override
            public void fail() throws HyracksDataException {
                pgw.fail();
            }

            @Override
            public void close() throws HyracksDataException {
                pgw.close();
            }

            @Override
            public void flush() throws HyracksDataException {
                pgw.flush();
            }
        };

    }
}
