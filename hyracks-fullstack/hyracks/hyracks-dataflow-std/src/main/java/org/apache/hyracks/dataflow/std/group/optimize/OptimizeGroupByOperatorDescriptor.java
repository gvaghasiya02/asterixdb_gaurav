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

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;

public class OptimizeGroupByOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private final int[] groupFields;
    private final boolean groupAll;
    private final int framesLimit;
    private final String aggType;
    private final IAggregatorDescriptorFactory aggregatorFactory;

    public OptimizeGroupByOperatorDescriptor(IOperatorDescriptorRegistry spec, int[] groupFields,
            RecordDescriptor recordDescriptor, boolean groupAll, int framesLimit, String aggType,
            IAggregatorDescriptorFactory aggregatorFactory) {
        super(spec, 1, 1);
        this.groupFields = groupFields;
        outRecDescs[0] = recordDescriptor;
        this.groupAll = groupAll;
        this.framesLimit = framesLimit;
        this.aggType = aggType;
        this.aggregatorFactory = aggregatorFactory;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            final IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions)
            throws HyracksDataException {
        return new OptimizeGroupByOperatorNodePushable(ctx, groupFields,
                recordDescProvider.getInputRecordDescriptor(getActivityId(), 0), outRecDescs[0], groupAll, framesLimit,
                aggType, aggregatorFactory);
    }
}
