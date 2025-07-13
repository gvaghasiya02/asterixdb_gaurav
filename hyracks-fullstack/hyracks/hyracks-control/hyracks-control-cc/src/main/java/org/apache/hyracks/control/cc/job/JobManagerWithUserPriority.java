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
package org.apache.hyracks.control.cc.job;

import java.util.Collections;
import java.util.List;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.api.job.resource.IJobCapacityController;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.application.CCServiceContext;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.control.common.work.IResultCallback;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JobManagerWithUserPriority extends JobManager {
    private static final Logger LOGGER = LogManager.getLogger();

    public JobManagerWithUserPriority(CCConfig ccConfig, ClusterControllerService ccs,
            IJobCapacityController jobCapacityController) {
        super(ccConfig, ccs, jobCapacityController);
    }

    @Override
    public void add(JobRun jobRun) throws HyracksException {
        //All newly added jobs should get queued
        checkJob(jobRun);
        JobSpecification job = jobRun.getJobSpecification();
        jobCapacityController.setJobSizeTag(jobRun.getJobSpecification());
        LOGGER.warn("SizeTag: " + jobRun.getJobSpecification().getSizeTag());
        IJobCapacityController.JobSubmissionStatus status =
                jobCapacityController.allocate(job, jobRun.getJobId(), jobRun.getFlags());
        CCServiceContext serviceCtx = ccs.getContext();
        serviceCtx.notifyJobCreation(jobRun.getJobId(), job, status);
        //        if (jobRun.getJobSpecification().getSizeTag() == JobSpecification.JobSizeTag.ZERO && jobCapacityController
        //                .allocate(jobRun.getJobSpecification()) == IJobCapacityController.JobSubmissionStatus.EXECUTE) {
        //            LOGGER.warn("Executed without queueing!");
        //            executeJob(jobRun);
        //            return;
        //        }
        queueJob(jobRun);
        //Whenever a new jobs added or a job finishes, check for jobs in the queue that can execite with the
        // current resources
        pickJobsToRun();
    }

    @Override
    public void cancel(JobId jobId, IResultCallback<Void> callback) throws HyracksException {
        // Cancels a running job.
        if (activeRunMap.containsKey(jobId)) {
            JobRun jobRun = activeRunMap.get(jobId);
            jobQueue.cancel(jobId);//Remove it from the running queue
            // The following call will abort all ongoing tasks and then consequently
            // trigger JobCleanupWork and JobCleanupNotificationWork which will update the lifecyle of the job.
            // Therefore, we do not remove the job out of activeRunMap here.
            jobRun.getExecutor().cancelJob(callback);
            return;
        }
        // Removes a pending job.
        JobRun jobRun = jobQueue.remove(jobId);
        if (jobRun != null) {
            List<Exception> exceptions =
                    Collections.singletonList(HyracksException.create(ErrorCode.JOB_CANCELED, jobId));
            // Since the job has not been executed, we only need to update its status and lifecyle here.
            jobRun.setStatus(JobStatus.FAILURE_BEFORE_EXECUTION, exceptions);
            runMapArchive.put(jobId, jobRun);
            runMapHistory.put(jobId, exceptions);
            CCServiceContext serviceCtx = ccs.getContext();
            if (serviceCtx != null) {
                try {
                    serviceCtx.notifyJobFinish(jobId, jobRun.getJobSpecification(), JobStatus.FAILURE_BEFORE_EXECUTION,
                            exceptions);
                } catch (Exception e) {
                    LOGGER.error("Exception notifying cancel on pending job {}", jobId, e);
                    throw HyracksDataException.create(e);
                }
            }
        }
        callback.setValue(null);
    }
}
