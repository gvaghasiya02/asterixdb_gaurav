/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.app.resource;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractGroupByPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractStableSortPOperator;
import org.apache.hyracks.algebricks.runtime.operators.std.SplitOperatorDescriptor;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.PlanStageTemp;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.group.external.ExternalGroupOperatorDescriptor;
import org.apache.hyracks.dataflow.std.group.sort.SortGroupByOperatorDescriptor;
import org.apache.hyracks.dataflow.std.join.OptimizedHybridHashJoinOperatorDescriptor;
import org.apache.hyracks.dataflow.std.misc.ReplicateOperatorDescriptor;
import org.apache.hyracks.dataflow.std.misc.SortForwardOperatorDescriptor;
import org.apache.hyracks.util.annotations.NotThreadSafe;

/**
 * Visits the operator first. Then, it visits all its inputs (pre-order traversal). When it visits an operator, it adds
 * the operator to the current stage. If the operator is a multi-stage operator, it also adds the operator to a queue
 * to re-visit the operator again to create the other stage.
 */
@NotThreadSafe
public class PlanStagesGenerator {

    private static final int JOIN_NON_BLOCKING_INPUT = 0;
    private static final int JOIN_BLOCKING_INPUT = 1;
    //    private static final int JOIN_NUM_INPUTS = 2;
    private static final int FORWARD_NON_BLOCKING_INPUT = 0;
    private static final int FORWARD_BLOCKING_INPUT = 1;
    //    private static final int FORWARD_NUM_INPUTS = 2;
    //    private final Set<ILogicalOperator> visitedOperators = new HashSet<>();
    //    private final LinkedList<ILogicalOperator> pendingMultiStageOperators = new LinkedList<>();
    private final Set<IOperatorDescriptor> visitedOperators = new HashSet<>();
    private final LinkedList<IOperatorDescriptor> pendingMultiStageOperators = new LinkedList<>();
    private final List<PlanStageTemp> stages = new ArrayList<>();
    private PlanStageTemp currentStage;
    private int stageCounter;
    private JobSpecification jobSpec;

    public PlanStagesGenerator(JobSpecification jobSpec) {
        this.jobSpec = jobSpec;
        currentStage = new PlanStageTemp(++stageCounter);
        stages.add(currentStage);
    }

    public List<PlanStageTemp> getStages() {
        return stages;
    }

    private void visitMultiStageOpOD(IOperatorDescriptor multiStageOp) throws AlgebricksException, HyracksException {
        final PlanStageTemp blockingOpStage = new PlanStageTemp(++stageCounter);
        blockingOpStage.getOperatorDescriptors().add(multiStageOp);
        stages.add(blockingOpStage);
        currentStage = blockingOpStage;
        if (multiStageOp instanceof OptimizedHybridHashJoinOperatorDescriptor) {
            // visit only the blocking input creating a new stage
            IConnectorDescriptor connector = jobSpec.getInputConnectorDescriptor(multiStageOp, JOIN_BLOCKING_INPUT);
            final IOperatorDescriptor joinBlockingInput = jobSpec.getProducer(connector);
            visit(joinBlockingInput);
        } else if (multiStageOp instanceof AbstractGroupByPOperator
                || multiStageOp instanceof AbstractStableSortPOperator) {
            //Order & GroupBy
            visitInputs(multiStageOp);
        } else if (multiStageOp instanceof SortForwardOperatorDescriptor) {
            IConnectorDescriptor connector = jobSpec.getInputConnectorDescriptor(multiStageOp, FORWARD_BLOCKING_INPUT);
            final IOperatorDescriptor forwardBlockingInput = jobSpec.getProducer(connector);
            visit(forwardBlockingInput);
        } else {
            throw new IllegalStateException("Unrecognized blocking operator: " + multiStageOp.getDisplayName());
        }
    }

    private void visitInputs(IOperatorDescriptor op) throws AlgebricksException, HyracksException {
        if (isMaterializedOD(op)) {
            // don't visit the inputs of this operator since it is supposed to be blocking due to materialization.
            // some other non-blocking operator will visit those inputs when reached.
            return;
        }
        for (int i = 0; i < op.getInputArity(); i++) {
            IConnectorDescriptor connector = jobSpec.getInputConnectorDescriptor(op, i);
            if (connector != null) {
                visit(jobSpec.getProducer(connector));
            }
        }
    }

    /**
     * Checks whether the operator {@code op} is supposed to be materialized
     * due to a replicate/split operators.
     *
     * @param op
     * @return true if the operator will be materialized. Otherwise false
     */
    private boolean isMaterializedOD(IOperatorDescriptor op) {
        int x = op.getInputArity();
        for (int i = 0; i < op.getInputArity(); i++) {
            IConnectorDescriptor conn = jobSpec.getInputConnectorDescriptor(op, i);
            if (conn != null) {
                IOperatorDescriptor input = jobSpec.getProducer(conn);
                if (input instanceof ReplicateOperatorDescriptor) {
                    final ReplicateOperatorDescriptor replOp = (ReplicateOperatorDescriptor) op;
                    if (replOp.isRequiredMaterialization()) {
                        return true;
                    }
                } else if (input instanceof SplitOperatorDescriptor) {
                    final SplitOperatorDescriptor splitOp = (SplitOperatorDescriptor) op;
                    if (splitOp.isRequiredMaterialization()) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Merges all operators on the current stage to the stage on which {@code op} appeared.
     *
     * @param op
     */
    private void merge(IOperatorDescriptor op) {
        // all operators in this stage belong to the stage of the already visited op
        for (PlanStageTemp stage : stages) {
            if (stage != currentStage && stage.getOperatorDescriptors().contains(op)) {
                stage.getOperatorDescriptors().addAll(currentStage.getOperatorDescriptors());
                stages.remove(currentStage);
                currentStage = stage;
                break;
            }
        }
    }

    private void addToStageOD(IOperatorDescriptor op) throws AlgebricksException, HyracksException {
        currentStage.getOperatorDescriptors().add(op);
        if (op instanceof OptimizedHybridHashJoinOperatorDescriptor) {
            pendingMultiStageOperators.add(op);
            // continue on the same stage
            IConnectorDescriptor connector = jobSpec.getInputConnectorDescriptor(op, JOIN_NON_BLOCKING_INPUT);
            final IOperatorDescriptor joinNonBlockingInput = jobSpec.getProducer(connector);
            visit(joinNonBlockingInput);
        } else if (op instanceof AbstractGroupByPOperator) {
            if (op instanceof SortGroupByOperatorDescriptor || op instanceof ExternalGroupOperatorDescriptor) {
                //Blocking GroupBy
                pendingMultiStageOperators.add(op);
                return;
            }
            // continue on the same stage
            visitInputs(op);
        } else if (op instanceof AbstractStableSortPOperator) {
            //Order
            pendingMultiStageOperators.add(op);
        } else if (op instanceof SortForwardOperatorDescriptor) {
            pendingMultiStageOperators.add(op);
            // continue on the same current stage through the branch that is non-blocking
            IConnectorDescriptor connector = jobSpec.getInputConnectorDescriptor(op, FORWARD_NON_BLOCKING_INPUT);
            final IOperatorDescriptor forwardNonBlockingInput = jobSpec.getProducer(connector);
            visit(forwardNonBlockingInput);
        } else {
            visitInputs(op);
        }
    }

    private void visitOp(IOperatorDescriptor op) throws HyracksException {
        if (op instanceof SplitOperatorDescriptor || op instanceof ReplicateOperatorDescriptor) {
            if (!visitedOperators.contains(op)) {
                visitedOperators.add(op);
                //visit(op);
            } else {
                merge(op);
                return;
            }
        }
        try {
            addToStageOD(op);
        } catch (AlgebricksException e) {
            e.printStackTrace();
        }
        if (!pendingMultiStageOperators.isEmpty()) {
            final IOperatorDescriptor firstPending = pendingMultiStageOperators.pop();
            try {
                visitMultiStageOpOD(firstPending);
            } catch (AlgebricksException e) {
                e.printStackTrace();
            }
        }
    }

    public void visit(IOperatorDescriptor op) throws HyracksException {
        //root

        visitOp(op);
        jobSpec.setStages(stages);

    }
}
