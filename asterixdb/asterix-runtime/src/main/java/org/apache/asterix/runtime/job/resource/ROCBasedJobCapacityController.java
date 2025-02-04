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

package org.apache.asterix.runtime.job.resource;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.api.application.ICCApplication;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.PlanStageTemp;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.job.resource.IClusterCapacity;
import org.apache.hyracks.control.cc.scheduler.IResourceManager;
import org.apache.hyracks.dataflow.std.join.OptimizedHybridHashJoinOperatorDescriptor;

public class ROCBasedJobCapacityController extends JobCapacityController {

    public ROCBasedJobCapacityController(IResourceManager resourceManager, ICCApplication ccApp) {
        super(resourceManager, ccApp);
    }

    public JobSubmissionStatus allocate(JobSpecification job) throws HyracksException {
        int maxMemory = 0;
        int maxCore = 0;
        IClusterCapacity currentCapacity = resourceManager.getCurrentCapacity();
        long currentAggregatedMemoryByteSize = currentCapacity.getAggregatedMemoryByteSize();
        int currentAggregatedAvailableCores = currentCapacity.getAggregatedCores();
        long largestStageMemoryNeeded = job.getLargestStageMemoryRequirements();
        //TODO(shiva): Note: Careful! for independent activity clusters you need to sum up their core requirements
        // since they run concurrently. Consider it also if you merge them as one stage, their core requirements
        // should still get summed up.
        if (largestStageMemoryNeeded < 0.25 * resourceManager.getMaximumCapacity().getAggregatedMemoryByteSize()
                && largestStageMemoryNeeded <= currentAggregatedMemoryByteSize) {
            //small query==>assign maximum needed memory to all joins
            for (PlanStageTemp stage : job.getStages()) {
                for (IOperatorDescriptor opDesc : stage.getOperatorDescriptors()) {//get the largest stage
                    if (opDesc.getInputSize() > 0) {
                        if (opDesc instanceof OptimizedHybridHashJoinOperatorDescriptor)
                            opDesc.setMemoryRequirements(opDesc.getMemoryRequirements());
                    }
                    currentAggregatedMemoryByteSize -= opDesc.getMemoryRequirements();
                }
            }
            resourceManager.getCurrentCapacity().setAggregatedMemoryByteSize(currentAggregatedMemoryByteSize);
            return JobSubmissionStatus.EXECUTE;
        } else {
            //For each stage, distribute the current memory. Be careful about the operators that have multiple phases
            // and participate in multi stages
            long maxAllowedMemoryForStage = (long) Math.min(currentAggregatedMemoryByteSize,
                    0.5 * resourceManager.getMaximumCapacity().getAggregatedMemoryByteSize());
            for (PlanStageTemp stage : job.getStages()) {
                Map<String, List<IOperatorDescriptor>> sizeToOpDesc = new HashMap<>();
                long usedMemory = 0;
                for (IOperatorDescriptor opDesc : stage.getOperatorDescriptors()) {//Assign minimum memory to each
                    // HHJ operator
                    if (opDesc.getInputSize() > 0) {//define if an operator is small,medium, or large
                        if (opDesc instanceof OptimizedHybridHashJoinOperatorDescriptor) {
                            opDesc.setMemoryRequirements((int) Math.ceil(Math.sqrt(opDesc.getMemoryRequirements())));
                            String opSize = getOpSize(opDesc);
                            if (!sizeToOpDesc.containsKey(opSize)) {
                                sizeToOpDesc.put(opSize, new LinkedList<>());
                            }
                            sizeToOpDesc.get(opSize).add(opDesc);
                        }
                    }
                    usedMemory += opDesc.getMemoryRequirements();
                    if (usedMemory > maxAllowedMemoryForStage) {//Either we can't satisfy it since the current available
                        // memory is less than 0.5*maxMemory (each query's limit)(==>Queue), or we can never
                        // satisfy it cause the minimum memory that the query requires is more than its limit
                        // (exception).
                        if (currentAggregatedMemoryByteSize > 0.5
                                * resourceManager.getMaximumCapacity().getAggregatedMemoryByteSize()) {
                            try {
                                throw new Exception("Not enough memory.");
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        } else {
                            return JobSubmissionStatus.QUEUE;
                        }
                    }

                }
                long leftOverMemory = maxAllowedMemoryForStage - usedMemory;
                if (leftOverMemory > 0) {
                    //add to the memory of memory-intensive operators by starting from smaller ones
                    leftOverMemory = addMoreMemory(sizeToOpDesc.get("Small"), leftOverMemory);
                }
                if (leftOverMemory > 0) {
                    //add to the memory of memory-intensive operators by starting from smaller ones
                    addMoreMemory(sizeToOpDesc.get("Medium"), leftOverMemory);
                }
            }
        }
        return JobSubmissionStatus.EXECUTE;
    }

    private long addMoreMemory(List<IOperatorDescriptor> opDescs, long leftOverMemory) {
        if (opDescs != null) {
            for (IOperatorDescriptor opDesc : opDescs) {
                long neededMemory = opDesc.getInputSize() - opDesc.getMemoryRequirements();
                if (neededMemory > 0 && leftOverMemory >= neededMemory) {
                    opDesc.setMemoryRequirements((opDesc.getMemoryRequirements() + neededMemory));
                    leftOverMemory -= neededMemory;
                } else {
                    break;
                }
            }
        }
        return leftOverMemory;
    }

    private String getOpSize(IOperatorDescriptor opDesc) {
        long maxMemory = resourceManager.getMaximumCapacity().getAggregatedMemoryByteSize();
        if (opDesc.getInputSize() < 0.25 * maxMemory) {
            return "Small";
        } else if (opDesc.getInputSize() <= maxMemory) {
            return "Medium";
        } else
            return "Large";
    }

    @Override
    public void release(JobSpecification job) {
        //TODO(shiva)
        IClusterCapacity requiredCapacity = job.getRequiredClusterCapacity();
        long reqAggregatedMemoryByteSize = requiredCapacity.getAggregatedMemoryByteSize();
        int reqAggregatedNumCores = requiredCapacity.getAggregatedCores();
        IClusterCapacity currentCapacity = resourceManager.getCurrentCapacity();
        long aggregatedMemoryByteSize = currentCapacity.getAggregatedMemoryByteSize();
        int aggregatedNumCores = currentCapacity.getAggregatedCores();
        currentCapacity.setAggregatedMemoryByteSize(aggregatedMemoryByteSize + reqAggregatedMemoryByteSize);
        currentCapacity.setAggregatedCores(aggregatedNumCores + reqAggregatedNumCores);
    }
}
