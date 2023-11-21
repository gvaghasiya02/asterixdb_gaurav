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

import java.nio.ByteBuffer;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;

class OptimizeGroupLOperatorNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {
    private final IHyracksTaskContext ctx;
    private final int[] groupFields;
    private final IBinaryComparatorFactory[] comparatorFactories;
    private final IAggregatorDescriptorFactory aggregatorFactory;
    private final RecordDescriptor inRecordDescriptor;
    private final RecordDescriptor outRecordDescriptor;
    private final boolean groupAll;
    private final int frameLimit;
    private final String aggType;

    private OptimizeGroupWriter ogw;

    OptimizeGroupLOperatorNodePushable(IHyracksTaskContext ctx, int[] groupFields,
            IBinaryComparatorFactory[] comparatorFactories, IAggregatorDescriptorFactory aggregatorFactory,
            RecordDescriptor inRecordDescriptor, RecordDescriptor outRecordDescriptor, boolean groupAll, int frameLimit,
            String aggType) {
        this.ctx = ctx;
        this.groupFields = groupFields;
        this.comparatorFactories = comparatorFactories;
        this.aggregatorFactory = aggregatorFactory;
        this.inRecordDescriptor = inRecordDescriptor;
        this.outRecordDescriptor = outRecordDescriptor;
        this.groupAll = groupAll;
        this.frameLimit = frameLimit;
        this.aggType = aggType;
    }

    @Override
    public void open() throws HyracksDataException {
        final IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; ++i) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }
        ogw = new OptimizeGroupWriter(ctx, groupFields, comparators, aggregatorFactory, inRecordDescriptor,
                outRecordDescriptor, writer, false, groupAll, frameLimit, aggType);
        ogw.open();
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        ogw.nextFrame(buffer);
    }

    @Override
    public void fail() throws HyracksDataException {
        ogw.fail();
    }

    @Override
    public void close() throws HyracksDataException {
        ogw.close();
    }

    @Override
    public void flush() throws HyracksDataException {
        //        pgw.flush();
    }
}
