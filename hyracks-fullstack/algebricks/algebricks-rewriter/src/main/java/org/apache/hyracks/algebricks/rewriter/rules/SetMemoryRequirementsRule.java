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

package org.apache.hyracks.algebricks.rewriter.rules;

import static java.lang.Math.ceil;

import java.util.Map;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.IPhysicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.OperatorAnnotations;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractUnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DelegateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistributeResultOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ForwardOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IndexInsertDeleteUpsertOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteUpsertOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IntersectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterUnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterUnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.MaterializeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ReplicateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.RunningAggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ScriptOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SinkOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SplitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SwitchOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.TokenizeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WindowOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WriteOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractStableSortPOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.algebricks.core.algebra.properties.LocalMemoryRequirements;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import org.apache.hyracks.api.exceptions.ErrorCode;

/**
 * Set memory requirements for all operators as follows:
 * <ol>
 * <li>First call {@link IPhysicalOperator#createLocalMemoryRequirements(ILogicalOperator, PhysicalOptimizationConfig)}
 *     to initialize each operator's {@link LocalMemoryRequirements} with minimal memory budget required by
 *     that operator</li>
 * <li>Then increase memory requirements for certain operators as specified by {@link PhysicalOptimizationConfig}</li>
 * </ol>
 */
public class SetMemoryRequirementsRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        IPhysicalOperator physOp = op.getPhysicalOperator();
        if (physOp.getLocalMemoryRequirements() != null) {
            return false;
        }
        computeLocalMemoryRequirements(op, createMemoryRequirementsConfigurator(context), context);
        return true;
    }

    private void computeLocalMemoryRequirements(AbstractLogicalOperator op,
            ILogicalOperatorVisitor<Void, Void> memoryRequirementsVisitor, IOptimizationContext context)
            throws AlgebricksException {
        IPhysicalOperator physOp = op.getPhysicalOperator();
        if (physOp.getLocalMemoryRequirements() == null) {
            if (memoryRequirementsVisitor == null) {
                // null means forcing the min memory budget from the physical optimization config
                physOp.createLocalMemoryRequirements(op, context.getPhysicalOptimizationConfig());
            } else {
                physOp.createLocalMemoryRequirements(op);
                if (physOp.getLocalMemoryRequirements() == null) {
                    throw new IllegalStateException(physOp.getOperatorTag().toString());
                }
                op.accept(memoryRequirementsVisitor, null);
            }
        }
        if (op.hasNestedPlans()) {
            AbstractOperatorWithNestedPlans nested = (AbstractOperatorWithNestedPlans) op;
            for (ILogicalPlan p : nested.getNestedPlans()) {
                for (Mutable<ILogicalOperator> root : p.getRoots()) {
                    computeLocalMemoryRequirements((AbstractLogicalOperator) root.getValue(), memoryRequirementsVisitor,
                            context);
                }
            }
        }
        for (Mutable<ILogicalOperator> opRef : op.getInputs()) {
            computeLocalMemoryRequirements((AbstractLogicalOperator) opRef.getValue(), memoryRequirementsVisitor,
                    context);
        }
    }

    protected ILogicalOperatorVisitor<Void, Void> createMemoryRequirementsConfigurator(IOptimizationContext context) {
        return new MemoryRequirementsConfigurator(context);
    }

    protected static class MemoryRequirementsConfigurator implements ILogicalOperatorVisitor<Void, Void> {

        protected final IOptimizationContext context;

        protected final PhysicalOptimizationConfig physConfig;

        protected int computationLocationsLength;

        protected MemoryRequirementsConfigurator(IOptimizationContext context) {
            this.context = context;
            this.physConfig = context.getPhysicalOptimizationConfig();
            //computation Location based on that memory gets divided.
            INodeDomain nodeDomain = context.getComputationNodeDomain();
            this.computationLocationsLength = nodeDomain != null ? nodeDomain.cardinality() : 1;
        }

        // helper methods

        protected void setOperatorMemoryBudget(AbstractLogicalOperator op, int CBOBasedMaxMemBudgetInFrames,
                int CBOBasedOptimalMemBudgetInFrames, int memBudgetInFrames) throws AlgebricksException {
            LocalMemoryRequirements memoryReqs = op.getPhysicalOperator().getLocalMemoryRequirements();
            int minBudgetInFrames = memoryReqs.getMinMemoryBudgetInFrames();
            if (memBudgetInFrames < minBudgetInFrames) {
                throw AlgebricksException.create(ErrorCode.ILLEGAL_MEMORY_BUDGET, op.getSourceLocation(),
                        op.getOperatorTag().toString(), memBudgetInFrames * physConfig.getFrameSize(),
                        minBudgetInFrames * physConfig.getFrameSize());
            }
            memoryReqs.setMemoryBudgetInFrames(memBudgetInFrames);
            if (physConfig.getCBOMode()) {
                if (CBOBasedMaxMemBudgetInFrames != -1 && CBOBasedOptimalMemBudgetInFrames != -1) {
                    CBOBasedMaxMemBudgetInFrames = Math.max(CBOBasedMaxMemBudgetInFrames, minBudgetInFrames);
                    CBOBasedOptimalMemBudgetInFrames = Math.max(CBOBasedOptimalMemBudgetInFrames, minBudgetInFrames);
                } else {
                    CBOBasedMaxMemBudgetInFrames = memBudgetInFrames;
                    CBOBasedOptimalMemBudgetInFrames = memBudgetInFrames;
                }
            } else {
                CBOBasedMaxMemBudgetInFrames = -1;
                CBOBasedOptimalMemBudgetInFrames = -1;
            }
            memoryReqs.setCBOMaxMemoryBudgetInFrames(CBOBasedMaxMemBudgetInFrames);
            memoryReqs.setCBOOptimalMemoryBudgetInFrames(CBOBasedOptimalMemBudgetInFrames);
        }

        // variable memory operators

        @Override
        public Void visitOrderOperator(OrderOperator op, Void arg) throws AlgebricksException {
            Double inputCardinality = null, inputSize = null, fudgeFactor = 1.3;
            for (Map.Entry<String, Object> anno : op.getAnnotations().entrySet()) {
                if (anno.getValue() != null && anno.getKey().equals(OperatorAnnotations.OP_INPUT_CARDINALITY)) {
                    inputCardinality = (Double) anno.getValue();
                } else if (anno.getValue() != null && anno.getKey().equals(OperatorAnnotations.OP_INPUT_DOCSIZE)) {
                    inputSize = (Double) anno.getValue();
                }
            }
            // estimated size of normalized key
            int sizeOfSortColumns = 0;
            if (op.getPhysicalOperator().getOperatorTag() == PhysicalOperatorTag.STABLE_SORT
                    || op.getPhysicalOperator().getOperatorTag() == PhysicalOperatorTag.MICRO_STABLE_SORT) {
                AbstractStableSortPOperator sortop = (AbstractStableSortPOperator) op.getPhysicalOperator();
                sizeOfSortColumns = sortop.getSortColumns() != null ? sortop.getSortColumns().length : 0;

            }
            int normalizeKeySize = (4 + sizeOfSortColumns) * 8;
            int CBOBasedMaxMemBudgetInFrames = -1;
            int CBOBasedOptimalMemBudgetInFrames = -1;
            if (!physConfig.getQueryCompilerSortMemoryKey() && inputCardinality != null && inputSize != null
                    && inputCardinality > 0 && inputSize > 0) {
                // use the cardinality and size to compute the memory budget
                double opCard = inputCardinality;
                double opSize = inputSize;
                if (op.getExecutionMode() == AbstractLogicalOperator.ExecutionMode.LOCAL
                        || op.getExecutionMode() == AbstractLogicalOperator.ExecutionMode.PARTITIONED) {
                    opCard /= computationLocationsLength;
                }
                // Calculating Memory
                CBOBasedMaxMemBudgetInFrames = (int) ceil(opCard * (opSize) * fudgeFactor / physConfig.getFrameSize());
                CBOBasedMaxMemBudgetInFrames +=
                        (int) ceil(normalizeKeySize * opCard * fudgeFactor / physConfig.getFrameSize());
                // Next Best Memory which is square root of max data size
                CBOBasedOptimalMemBudgetInFrames = (int) ceil(Math.sqrt(CBOBasedMaxMemBudgetInFrames));
                // Added Extra Frames to the budget to account for the sort operator usually generator minus 1 frame from actual data.
                CBOBasedMaxMemBudgetInFrames += 2;
                CBOBasedOptimalMemBudgetInFrames += 2;
                CBOBasedOptimalMemBudgetInFrames =
                        Math.min(CBOBasedOptimalMemBudgetInFrames, physConfig.getMaxCBOSortFrames());
                CBOBasedMaxMemBudgetInFrames = Math.min(CBOBasedMaxMemBudgetInFrames, physConfig.getMaxCBOSortFrames());
            }
            setOperatorMemoryBudget(op, CBOBasedMaxMemBudgetInFrames, CBOBasedOptimalMemBudgetInFrames,
                    physConfig.getMaxFramesExternalSort());
            return null;
        }

        @Override
        public Void visitGroupByOperator(GroupByOperator op, Void arg) throws AlgebricksException {
            Double inputCardinality = null, outputCardinality = null, inputSize = null, outputSize = null,
                    fudgeFactor = 1.3;
            for (Map.Entry<String, Object> anno : op.getAnnotations().entrySet()) {
                if (anno.getValue() != null && anno.getKey().equals(OperatorAnnotations.OP_INPUT_CARDINALITY)) {
                    inputCardinality = (Double) anno.getValue();
                } else if (anno.getValue() != null && anno.getKey().equals(OperatorAnnotations.OP_OUTPUT_CARDINALITY)) {
                    outputCardinality = (Double) anno.getValue();
                } else if (anno.getValue() != null && anno.getKey().equals(OperatorAnnotations.OP_INPUT_DOCSIZE)) {
                    inputSize = (Double) anno.getValue();
                } else if (anno.getValue() != null && anno.getKey().equals(OperatorAnnotations.OP_OUTPUT_DOCSIZE)) {
                    outputSize = (Double) anno.getValue();
                }
            }
            int CBOBasedMaxMemBudgetInFrames = -1;
            int CBOBasedOptimalMemBudgetInFrames = -1;
            if (!physConfig.getQueryCompilerGroupMemoryKey() && inputCardinality != null && outputCardinality != null
                    && inputSize != null && outputSize != null && inputCardinality > 0 && inputSize > 0
                    && outputCardinality > 0 && outputSize > 0) {
                // use the cardinality and size to compute the memory budget
                if (op.getExecutionMode() == AbstractLogicalOperator.ExecutionMode.LOCAL
                        || op.getExecutionMode() == AbstractLogicalOperator.ExecutionMode.PARTITIONED) {
                    inputCardinality /= computationLocationsLength;
                }
                // Group By Max Memory is input size
                CBOBasedMaxMemBudgetInFrames =
                        (int) ceil(inputCardinality * inputSize * fudgeFactor / physConfig.getFrameSize());
                CBOBasedOptimalMemBudgetInFrames =
                        (int) ceil(outputCardinality * outputSize * fudgeFactor / physConfig.getFrameSize());
                // added extra frames to the budget to account for the group by operator (Hash based has minimum 1 input + 1 output + 2 hash table frames)
                CBOBasedMaxMemBudgetInFrames += 3;
                CBOBasedOptimalMemBudgetInFrames += 3;
                CBOBasedOptimalMemBudgetInFrames =
                        Math.min(CBOBasedOptimalMemBudgetInFrames, physConfig.getMaxCBOGroupFrames());
                CBOBasedMaxMemBudgetInFrames =
                        Math.min(CBOBasedMaxMemBudgetInFrames, physConfig.getMaxCBOGroupFrames());
            }
            setOperatorMemoryBudget(op, CBOBasedMaxMemBudgetInFrames, CBOBasedOptimalMemBudgetInFrames,
                    physConfig.getMaxFramesForGroupBy());
            return null;
        }

        @Override
        public Void visitWindowOperator(WindowOperator op, Void arg) throws AlgebricksException {
            if (op.getPhysicalOperator().getOperatorTag() == PhysicalOperatorTag.WINDOW) {
                Double inputCardinality = null, outputCardinality = null, inputSize = null, outputSize = null,
                        fudgeFactor = 1.3;
                for (Map.Entry<String, Object> anno : op.getAnnotations().entrySet()) {
                    if (anno.getValue() != null && anno.getKey().equals(OperatorAnnotations.OP_INPUT_CARDINALITY)) {
                        inputCardinality = (Double) anno.getValue();
                    } else if (anno.getValue() != null
                            && anno.getKey().equals(OperatorAnnotations.OP_OUTPUT_CARDINALITY)) {
                        outputCardinality = (Double) anno.getValue();
                    } else if (anno.getValue() != null && anno.getKey().equals(OperatorAnnotations.OP_INPUT_DOCSIZE)) {
                        inputSize = (Double) anno.getValue();
                    } else if (anno.getValue() != null && anno.getKey().equals(OperatorAnnotations.OP_OUTPUT_DOCSIZE)) {
                        outputSize = (Double) anno.getValue();
                    }
                }
                int CBOBasedMaxMemBudgetInFrames = -1;
                int CBOBasedOptimalMemBudgetInFrames = -1;
                if (!physConfig.getQueryCompilerWindowMemoryKey() && inputCardinality != null
                        && outputCardinality != null && inputSize != null && outputSize != null && inputCardinality > 0
                        && inputSize > 0 && outputCardinality > 0 && outputSize > 0) {
                    // use the cardinality and size to compute the memory budget
                    double inputTotalSize = inputCardinality * inputSize;
                    double outputTotalSize = outputCardinality * outputSize;
                    if (op.getExecutionMode() == AbstractLogicalOperator.ExecutionMode.LOCAL
                            || op.getExecutionMode() == AbstractLogicalOperator.ExecutionMode.PARTITIONED) {
                        inputTotalSize /= computationLocationsLength;
                        outputTotalSize /= computationLocationsLength;
                    }
                    CBOBasedMaxMemBudgetInFrames = (int) ceil(
                            Math.max(inputTotalSize, outputTotalSize) * fudgeFactor / physConfig.getFrameSize());
                    CBOBasedOptimalMemBudgetInFrames = (int) ceil(
                            Math.min(inputTotalSize, outputTotalSize) * fudgeFactor / physConfig.getFrameSize());
                    // added extra frames to the budget to account for the window operator (Minn frame requires are 5 frames)
                    CBOBasedMaxMemBudgetInFrames += 4;
                    CBOBasedOptimalMemBudgetInFrames += 4;
                    CBOBasedOptimalMemBudgetInFrames =
                            Math.min(CBOBasedOptimalMemBudgetInFrames, physConfig.getMaxCBOWindowFrames());
                    CBOBasedMaxMemBudgetInFrames =
                            Math.min(CBOBasedMaxMemBudgetInFrames, physConfig.getMaxCBOWindowFrames());
                }
                setOperatorMemoryBudget(op, CBOBasedMaxMemBudgetInFrames, CBOBasedOptimalMemBudgetInFrames,
                        physConfig.getMaxFramesForWindow());
            }
            return null;
        }

        @Override
        public Void visitInnerJoinOperator(InnerJoinOperator op, Void arg) throws AlgebricksException {
            return visitJoinOperator(op, arg);
        }

        @Override
        public Void visitLeftOuterJoinOperator(LeftOuterJoinOperator op, Void arg) throws AlgebricksException {
            return visitJoinOperator(op, arg);
        }

        protected Void visitJoinOperator(AbstractBinaryJoinOperator op, Void arg) throws AlgebricksException {
            Double buildCardinality = null, buildDocSize = null, outputCardinality = null, outputSize = null,
                    fudgeFactor = 1.3;
            for (Map.Entry<String, Object> anno : op.getAnnotations().entrySet()) {
                if (anno.getValue() != null && anno.getKey().equals(OperatorAnnotations.OP_BUILD_CARDINALITY)) {
                    buildCardinality = (Double) anno.getValue();
                } else if (anno.getValue() != null && anno.getKey().equals(OperatorAnnotations.OP_BUILD_DOCSIZE)) {
                    buildDocSize = (Double) anno.getValue();
                } else if (anno.getValue() != null && anno.getKey().equals(OperatorAnnotations.OP_OUTPUT_CARDINALITY)) {
                    outputCardinality = (Double) anno.getValue();
                } else if (anno.getValue() != null && anno.getKey().equals(OperatorAnnotations.OP_OUTPUT_DOCSIZE)) {
                    outputSize = (Double) anno.getValue();
                }
            }
            int CBOBasedMaxMemBudgetInFrames = -1;
            int CBOBasedOptimalMemBudgetInFrames = -1;
            if (!physConfig.getQueryCompilerJoinMemoryKey() && buildCardinality != null && buildDocSize != null
                    && outputSize != null && outputCardinality != null && outputCardinality > 0 && outputSize > 0
                    && buildCardinality > 0 && buildDocSize > 0) {
                // use the cardinality and size to compute the memory budget
                if (op.getExecutionMode() == AbstractLogicalOperator.ExecutionMode.LOCAL
                        || op.getExecutionMode() == AbstractLogicalOperator.ExecutionMode.PARTITIONED) {
                    buildCardinality /= computationLocationsLength;

                }
                // use the cardinality and size to compute the memory budget 40 bytes are for hash table size check simpleSerializableHashTable
                CBOBasedMaxMemBudgetInFrames =
                        (int) ceil((buildCardinality * (buildDocSize) * fudgeFactor) / physConfig.getFrameSize());
                CBOBasedMaxMemBudgetInFrames = Math.max(CBOBasedMaxMemBudgetInFrames, 2);
                //Calculation for Hash table
                CBOBasedMaxMemBudgetInFrames += (int) ceil((buildCardinality * (8)) / physConfig.getFrameSize());
                CBOBasedMaxMemBudgetInFrames += (int) ceil((buildCardinality * (32)) / physConfig.getFrameSize());
                CBOBasedOptimalMemBudgetInFrames = (int) ceil(Math.sqrt(CBOBasedMaxMemBudgetInFrames));
                // added extra frames to the budget to account for the join operator (Hash based has minimum 1 input + 1 output)
                CBOBasedMaxMemBudgetInFrames += 2;
                CBOBasedOptimalMemBudgetInFrames += 2;
                CBOBasedOptimalMemBudgetInFrames =
                        Math.min(CBOBasedOptimalMemBudgetInFrames, physConfig.getMaxCBOJoinFrames());
                CBOBasedMaxMemBudgetInFrames = Math.min(CBOBasedMaxMemBudgetInFrames, physConfig.getMaxCBOJoinFrames());
            }
            setOperatorMemoryBudget(op, CBOBasedMaxMemBudgetInFrames, CBOBasedOptimalMemBudgetInFrames,
                    physConfig.getMaxFramesForJoin());
            return null;
        }

        @Override
        public Void visitUnnestMapOperator(UnnestMapOperator op, Void arg) throws AlgebricksException {
            return visitAbstractUnnestMapOperator(op, arg);
        }

        @Override
        public Void visitLeftOuterUnnestMapOperator(LeftOuterUnnestMapOperator op, Void arg)
                throws AlgebricksException {
            return visitAbstractUnnestMapOperator(op, arg);
        }

        protected Void visitAbstractUnnestMapOperator(AbstractUnnestMapOperator op, Void arg)
                throws AlgebricksException {
            IPhysicalOperator physOp = op.getPhysicalOperator();
            if (physOp.getOperatorTag() == PhysicalOperatorTag.LENGTH_PARTITIONED_INVERTED_INDEX_SEARCH
                    || physOp.getOperatorTag() == PhysicalOperatorTag.SINGLE_PARTITION_INVERTED_INDEX_SEARCH) {
                Double inputCardinality = null, inputSize = null, fudgeFactor = 1.3;
                for (Map.Entry<String, Object> anno : op.getAnnotations().entrySet()) {
                    if (anno.getValue() != null && anno.getKey().equals(OperatorAnnotations.OP_INPUT_CARDINALITY)) {
                        inputCardinality = (Double) anno.getValue();
                    } else if (anno.getValue() != null && anno.getKey().equals(OperatorAnnotations.OP_INPUT_DOCSIZE)) {
                        inputSize = (Double) anno.getValue();
                    }
                }
                int CBOBasedMaxMemBudgetInFrames = -1, CBOBasedOptimalMemBudgetInFrames = -1;
                if (!physConfig.getQueryCompilerTextSearchMemoryKey() && inputCardinality != null && inputSize != null
                        && inputCardinality > 0 && inputSize > 0) {
                    if (op.getExecutionMode() == AbstractLogicalOperator.ExecutionMode.LOCAL
                            || op.getExecutionMode() == AbstractLogicalOperator.ExecutionMode.PARTITIONED) {
                        inputCardinality /= computationLocationsLength;
                    }
                    // use the cardinality and size to compute the memory budget
                    double opCard = inputCardinality;
                    double opSize = inputSize;
                    if (op.getExecutionMode() == AbstractLogicalOperator.ExecutionMode.LOCAL
                            || op.getExecutionMode() == AbstractLogicalOperator.ExecutionMode.PARTITIONED) {
                        opCard /= computationLocationsLength;
                    }
                    CBOBasedMaxMemBudgetInFrames =
                            (int) ceil(opCard * opSize * fudgeFactor / physConfig.getFrameSize());
                    CBOBasedMaxMemBudgetInFrames += 4;
                    CBOBasedOptimalMemBudgetInFrames = CBOBasedMaxMemBudgetInFrames;
                    CBOBasedOptimalMemBudgetInFrames =
                            Math.min(CBOBasedOptimalMemBudgetInFrames, physConfig.getMaxCBOTextSearchFrames());
                    CBOBasedMaxMemBudgetInFrames =
                            Math.min(CBOBasedMaxMemBudgetInFrames, physConfig.getMaxCBOTextSearchFrames());
                }
                setOperatorMemoryBudget(op, CBOBasedMaxMemBudgetInFrames, CBOBasedOptimalMemBudgetInFrames,
                        physConfig.getMaxFramesForTextSearch());
            }
            return null;
        }

        // fixed memory operators

        @Override
        public Void visitAggregateOperator(AggregateOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitRunningAggregateOperator(RunningAggregateOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitEmptyTupleSourceOperator(EmptyTupleSourceOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitLimitOperator(LimitOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitNestedTupleSourceOperator(NestedTupleSourceOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitAssignOperator(AssignOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitSelectOperator(SelectOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitDelegateOperator(DelegateOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitProjectOperator(ProjectOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitReplicateOperator(ReplicateOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitSplitOperator(SplitOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitSwitchOperator(SwitchOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitMaterializeOperator(MaterializeOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitScriptOperator(ScriptOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitSubplanOperator(SubplanOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitSinkOperator(SinkOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitUnionOperator(UnionAllOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitIntersectOperator(IntersectOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitUnnestOperator(UnnestOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitLeftOuterUnnestOperator(LeftOuterUnnestOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitDataScanOperator(DataSourceScanOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitDistinctOperator(DistinctOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitExchangeOperator(ExchangeOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitWriteOperator(WriteOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitDistributeResultOperator(DistributeResultOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitInsertDeleteUpsertOperator(InsertDeleteUpsertOperator op, Void arg)
                throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitIndexInsertDeleteUpsertOperator(IndexInsertDeleteUpsertOperator op, Void arg)
                throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitTokenizeOperator(TokenizeOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitForwardOperator(ForwardOperator op, Void arg) throws AlgebricksException {
            return null;
        }
    }
}
