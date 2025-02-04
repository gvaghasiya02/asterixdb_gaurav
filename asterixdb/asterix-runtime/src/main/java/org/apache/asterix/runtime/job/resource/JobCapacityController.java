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

import java.util.Set;

import org.apache.hyracks.api.application.ICCApplication;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.job.resource.IClusterCapacity;
import org.apache.hyracks.api.job.resource.IJobCapacityController;
import org.apache.hyracks.api.job.resource.IReadOnlyClusterCapacity;
import org.apache.hyracks.control.cc.scheduler.IResourceManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// To avoid the computation cost for checking the capacity constraint for each node,
// currently the admit/allocation decisions are based on the aggregated resource information.
// TODO(buyingyi): investigate partition-aware resource control.
public class JobCapacityController implements IJobCapacityController {

    private static final Logger LOGGER = LogManager.getLogger();
    final IResourceManager resourceManager;
    private final ICCApplication ccApp;

    public JobCapacityController(IResourceManager resourceManager, ICCApplication ccApp) {
        this.resourceManager = resourceManager;
        this.ccApp = ccApp;
    }

    @Override
    public JobSubmissionStatus allocate(JobSpecification job, JobId jobId, Set<JobFlag> jobFlags)
            throws HyracksException {
        if (!ccApp.acceptingJobs(jobFlags)) {
            throw HyracksDataException.create(ErrorCode.JOB_REJECTED, jobId);
        }
        IClusterCapacity requiredCapacity = job.getRequiredClusterCapacity();
        long reqAggregatedMemoryByteSize = requiredCapacity.getAggregatedMemoryByteSize();
        int reqAggregatedNumCores = requiredCapacity.getAggregatedCores();
        IReadOnlyClusterCapacity maximumCapacity = resourceManager.getMaximumCapacity();
        if (!(reqAggregatedMemoryByteSize <= maximumCapacity.getAggregatedMemoryByteSize()
                && reqAggregatedNumCores <= maximumCapacity.getAggregatedCores())) {
            throw HyracksException.create(ErrorCode.JOB_REQUIREMENTS_EXCEED_CAPACITY, requiredCapacity.toString(),
                    maximumCapacity.toString());
        }
        IClusterCapacity currentCapacity = resourceManager.getCurrentCapacity();
        long currentAggregatedMemoryByteSize = currentCapacity.getAggregatedMemoryByteSize();
        int currentAggregatedAvailableCores = currentCapacity.getAggregatedCores();
        if (!(reqAggregatedMemoryByteSize <= currentAggregatedMemoryByteSize
                && reqAggregatedNumCores <= currentAggregatedAvailableCores)) {
            return JobSubmissionStatus.QUEUE;
        }
        LOGGER.warn("Before Allocation: " + currentCapacity.getAggregatedCores() + " cores "
                + currentCapacity.getAggregatedMemoryByteSize() / 1024 / 1024 + " MB");
        currentCapacity.setAggregatedMemoryByteSize(currentAggregatedMemoryByteSize - reqAggregatedMemoryByteSize);
        currentCapacity.setAggregatedCores(currentAggregatedAvailableCores - reqAggregatedNumCores);
        LOGGER.warn("After Allocation: " + currentCapacity.getAggregatedCores() + " cores "
                + currentCapacity.getAggregatedMemoryByteSize() / 1024 / 1024 + " MB");
        return JobSubmissionStatus.EXECUTE;
    }

    @Override
    public void release(JobSpecification job) {
        IClusterCapacity requiredCapacity = job.getRequiredClusterCapacity();
        long reqAggregatedMemoryByteSize = requiredCapacity.getAggregatedMemoryByteSize();
        int reqAggregatedNumCores = requiredCapacity.getAggregatedCores();
        LOGGER.warn("Job " + job.getSizeTag() + " is finishing...Required Resources: "
                + (double) reqAggregatedMemoryByteSize / 1024 / 1024 + " MB of memory and " + reqAggregatedNumCores
                + " CPU cores.");
        IClusterCapacity currentCapacity = resourceManager.getCurrentCapacity();
        long aggregatedMemoryByteSize = currentCapacity.getAggregatedMemoryByteSize();
        int aggregatedNumCores = currentCapacity.getAggregatedCores();
        LOGGER.warn("Resources Before Release: " + (double) aggregatedMemoryByteSize / 1024 / 1024
                + " MB of memory and " + aggregatedNumCores + " CPU cores.");
        currentCapacity.setAggregatedMemoryByteSize(aggregatedMemoryByteSize + reqAggregatedMemoryByteSize);
        currentCapacity.setAggregatedCores(aggregatedNumCores + reqAggregatedNumCores);
        LOGGER.warn("Resources After Release: "
                + (double) resourceManager.getCurrentCapacity().getAggregatedMemoryByteSize() / 1024 / 1024
                + " MB of memory and " + resourceManager.getCurrentCapacity().getAggregatedCores() + " CPU cores.");
        ensureMaxCapacity();
    }

    @Override
    public void setJobSizeTag(JobSpecification job) {
        double memRatio = getMemoryRatio(job);
        LOGGER.warn(memRatio);
        if (memRatio <= 0.05) {
            String uid = job.getUserID();
            if (uid != null) {
                if (uid.contains("ZERO_Short")) {
                    job.setSizeTag(JobSpecification.JobSizeTag.ZERO_SHORT);
                } else if (uid.contains("ZERO_Long")) {
                    job.setSizeTag(JobSpecification.JobSizeTag.ZERO_LONG);
                }
            } else {
                job.setSizeTag(JobSpecification.JobSizeTag.ZERO);
            }
        } else if (memRatio <= 0.25) {
            job.setSizeTag(JobSpecification.JobSizeTag.SMALL);
        } else if (memRatio <= 0.75) {
            job.setSizeTag(JobSpecification.JobSizeTag.MEDIUM);
        } else {
            job.setSizeTag(JobSpecification.JobSizeTag.LARGE);
        }

    }

    @Override
    public int getNumberOfAvailableCores() {
        return resourceManager.getCurrentCapacity().getAggregatedCores();
    }

    @Override
    public double getMemoryRatio(JobSpecification job) {
        return (double) job.getRequiredClusterCapacity().getAggregatedMemoryByteSize()
                / resourceManager.getMaximumCapacity().getAggregatedMemoryByteSize();
    }

    @Override
    public IReadOnlyClusterCapacity getClusterCapacity() {
        return resourceManager.getCurrentCapacity();
    }

    private void ensureMaxCapacity() {
        final IClusterCapacity currentCapacity = resourceManager.getCurrentCapacity();
        final IReadOnlyClusterCapacity maximumCapacity = resourceManager.getMaximumCapacity();
        if (currentCapacity.getAggregatedCores() > maximumCapacity.getAggregatedCores()
                || currentCapacity.getAggregatedMemoryByteSize() > maximumCapacity.getAggregatedMemoryByteSize()) {
            LOGGER.warn("Current cluster available capacity {} is more than its maximum capacity {}", currentCapacity,
                    maximumCapacity);
        }
    }

    @Override
    public boolean hasEnoughMemory(long avgMemoryUsage) {
        return resourceManager.getCurrentCapacity().getAggregatedMemoryByteSize() >= avgMemoryUsage;
    };
}
