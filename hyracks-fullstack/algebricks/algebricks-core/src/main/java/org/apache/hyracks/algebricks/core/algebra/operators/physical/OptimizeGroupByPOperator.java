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
package org.apache.hyracks.algebricks.core.algebra.operators.physical;

import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionRuntimeProvider;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.operators.aggreg.SimpleAlgebricksAccumulatingAggregatorFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.group.AbstractAggregatorDescriptorFactory;
import org.apache.hyracks.dataflow.std.group.optimize.OptimizeGroupByOperatorDescriptor;

public class OptimizeGroupByPOperator extends AbstractPreclusteredGroupByPOperator {

    private final boolean groupAll;

    public OptimizeGroupByPOperator(List<LogicalVariable> columnList, boolean groupAll) {
        super(columnList);
        this.groupAll = groupAll;
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.OPTIMIZE_GROUP_BY;
    }

    @Override
    public boolean isMicroOperator() {
        return false;
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema opSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        GroupByOperator gby = (GroupByOperator) op;
        checkGroupAll(gby);
        ILogicalPlan p0 = gby.getNestedPlans().get(0);
        if (p0.getRoots().size() != 1) {
            throw new AlgebricksException(
                    "Optimize group-by currently works only for one nested plan with one root containing"
                            + "an aggregate and a nested-tuple-source.");
        }
        Mutable<ILogicalOperator> r0 = p0.getRoots().get(0);
        AggregateOperator aggOp = (AggregateOperator) r0.getValue();
        if (aggOp.getExpressions().size() > 1) {
            throw new AlgebricksException("Optimize group-by currently works only for one aggregate on projection");
        }
        String aggOpType = aggOp.getExpressions().get(0).getValue().toString();
        String aggType;
        if (aggOpType.contains("sql-count"))
            aggType = "COUNT";
        else if (aggOpType.contains("sql-sum"))
            aggType = "SUM";
        else if (aggOpType.contains("sql-max"))
            aggType = "MAX";
        else if (aggOpType.contains("sql-min"))
            aggType = "MIN";
        else {
            throw new AlgebricksException("Optimize group-by currently not supporting average");
        }
        int i = 0;
        int keys[] = JobGenHelper.variablesToFieldIndexes(columnList, inputSchemas[0]);
        int fdColumns[] = getFdColumns(gby, inputSchemas[0]);
        int[] keyAndDecFields = new int[keys.length + fdColumns.length];
        for (i = 0; i < keys.length; ++i) {
            keyAndDecFields[i] = keys[i];
        }
        for (i = 0; i < fdColumns.length; i++) {
            keyAndDecFields[keys.length + i] = fdColumns[i];
        }
        // compile subplans and set the gby op. schema accordingly
        compileSubplans(inputSchemas[0], gby, opSchema, context);
        IOperatorDescriptorRegistry spec = builder.getJobSpec();
        RecordDescriptor recordDescriptor =
                JobGenHelper.mkRecordDescriptor(context.getTypeEnvironment(op), opSchema, context);
        int framesLimit = localMemoryRequirements.getMemoryBudgetInFrames();
        int n = aggOp.getExpressions().size();
        IAggregateEvaluatorFactory[] aff = new IAggregateEvaluatorFactory[n];
        i = 0;
        IExpressionRuntimeProvider expressionRuntimeProvider = context.getExpressionRuntimeProvider();
        IVariableTypeEnvironment aggOpInputEnv = context.getTypeEnvironment(aggOp.getInputs().get(0).getValue());
        for (Mutable<ILogicalExpression> exprRef : aggOp.getExpressions()) {
            AggregateFunctionCallExpression aggFun = (AggregateFunctionCallExpression) exprRef.getValue();
            aff[i++] = expressionRuntimeProvider.createAggregateFunctionFactory(aggFun, aggOpInputEnv, inputSchemas,
                    context);
        }

        AbstractAggregatorDescriptorFactory aggregatorFactory =
                new SimpleAlgebricksAccumulatingAggregatorFactory(aff, keyAndDecFields);
        aggregatorFactory.setSourceLocation(gby.getSourceLocation());
        OptimizeGroupByOperatorDescriptor opDesc = new OptimizeGroupByOperatorDescriptor(spec, keyAndDecFields,
                recordDescriptor, groupAll, framesLimit, aggType, aggregatorFactory);
        opDesc.setSourceLocation(gby.getSourceLocation());

        contributeOpDesc(builder, gby, opDesc);

        ILogicalOperator src = op.getInputs().get(0).getValue();
        builder.contributeGraphEdge(src, 0, op, 0);
    }

    @Override
    public String toString() {
        return getOperatorTag().toString() + (groupAll ? "(ALL)" : "") + columnList;
    }
}
