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

package org.apache.hyracks.algebricks.runtime.operators.win;

import java.util.Arrays;

import org.apache.hyracks.algebricks.runtime.base.IRunningAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputOneFramePushRuntime;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.dataflow.std.buffermanager.CBOMemoryBudget;

/**
 * Runtime factory for window operators that performs partition materialization and evaluates running aggregates
 * that require information about number of tuples in the partition.
 */
public class WindowMaterializingRuntimeFactory extends AbstractWindowRuntimeFactory {

    private static final long serialVersionUID = 1L;
    final CBOMemoryBudget cboMemoryBudget;

    int memSizeInFrames;

    public WindowMaterializingRuntimeFactory(int[] partitionColumns,
            IBinaryComparatorFactory[] partitionComparatorFactories,
            IBinaryComparatorFactory[] orderComparatorFactories, int[] projectionColumnsExcludingSubplans,
            int[] runningAggOutColumns, IRunningAggregateEvaluatorFactory[] runningAggFactories,
            CBOMemoryBudget cboMemoryBudget) {
        super(partitionColumns, partitionComparatorFactories, orderComparatorFactories,
                projectionColumnsExcludingSubplans, runningAggOutColumns, runningAggFactories);
        this.cboMemoryBudget = cboMemoryBudget;
        this.memSizeInFrames = cboMemoryBudget.sizeInFrames();
    }

    @Override
    public AbstractOneInputOneOutputOneFramePushRuntime createOneOutputPushRuntime(IHyracksTaskContext ctx) {
        if (ctx.getJobFlags().contains(JobFlag.USE_CBO_MAX_MEMORY) && cboMemoryBudget.cboMaxSizeInFrames() > 0) {
            memSizeInFrames = cboMemoryBudget.cboMaxSizeInFrames();
        }
        if (ctx.getJobFlags().contains(JobFlag.USE_CBO_OPTIMAL_MEMORY)
                && cboMemoryBudget.cboOptimalSizeInFrames() > 0) {
            memSizeInFrames = cboMemoryBudget.cboOptimalSizeInFrames();
        }
        return new WindowMaterializingPushRuntime(partitionColumns, partitionComparatorFactories,
                orderComparatorFactories, projectionList, runningAggOutColumns, runningAggFactories, ctx,
                memSizeInFrames, sourceLoc);
    }

    @Override
    public String toString() {
        return "window [materialize] (" + Arrays.toString(partitionColumns) + ") "
                + Arrays.toString(runningAggOutColumns) + " := " + Arrays.toString(runningAggFactories);
    }
}
