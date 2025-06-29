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
package org.apache.hyracks.control.cc.scheduler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.api.job.resource.IJobCapacityController;
import org.apache.hyracks.control.cc.job.IJobManager;
import org.apache.hyracks.control.cc.job.JobRun;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FairToUsersQueue implements IJobQueue {
    private static final Logger LOGGER = LogManager.getLogger();

    private final PriorityBlockingQueue<JobRun> jobQueue;
    private final IJobManager jobManager;
    private final IJobCapacityController jobCapacityController;
    private final int jobQueueCapacity;
    private final HashMap<String, Double> userToUsage;
    private Double totalUsage;
    private final HashMap<JobId, JobRun> jobIDToJobRun;

    public FairToUsersQueue(IJobManager jobManager, IJobCapacityController jobCapacityController) {
        this.jobManager = jobManager;
        this.totalUsage = 0D;
        this.jobCapacityController = jobCapacityController;
        this.jobQueueCapacity = jobManager.getJobQueueCapacity();
        this.userToUsage = new HashMap<>();
        this.jobIDToJobRun = new HashMap<>();
        this.jobQueue = new PriorityBlockingQueue<>(jobQueueCapacity, new Comparator<JobRun>() {
            @Override
            public int compare(JobRun o1, JobRun o2) {
                ////                //String o1User = o1.getJobSpecification().getUsername();
                ////                Double o1Usage = userToUsage.containsKey(o1User) ? userToUsage.get(o1User) : 0;
                ////                String o2User = o2.getJobSpecification().getUsername();
                //                Double o2Usage = userToUsage.containsKey(o2User) ? userToUsage.get(o2User) : 0;
                //                return Double.compare(o1Usage / totalUsage, o2Usage / totalUsage);
                return 0;
            }
        });
    }

    @Override
    public void add(JobRun run) throws HyracksException {
        run.setAddedToQueueTime(System.nanoTime());
        if (!jobQueue.add(run)) {
            throw HyracksException.create(ErrorCode.JOB_QUEUE_FULL, jobQueueCapacity);
        }
        jobIDToJobRun.put(run.getJobId(), run);
    }

    @Override
    public JobRun remove(JobId jobId) {
        JobRun jr = jobIDToJobRun.get(jobId);
        jobQueue.remove(jr);
        jobIDToJobRun.remove(jobId);
        return jr;
    }

    @Override
    public JobRun get(JobId jobId) {
        return jobIDToJobRun.get(jobId);
    }

    @Override
    public synchronized List<JobRun> pull() {
        List<JobRun> jobRuns = new LinkedList<>();
        int i = 0;
        while (i < jobQueue.size()) {
            JobRun run = jobQueue.peek();
            JobSpecification job = run.getJobSpecification();
            // Cluster maximum capacity can change over time, thus we have to re-check if the job should be rejected
            // or not.
            try {
                IJobCapacityController.JobSubmissionStatus status =
                        jobCapacityController.allocate(job, run.getJobId(), run.getFlags());
                // Checks if the job can be executed immediately.
                if (status == IJobCapacityController.JobSubmissionStatus.EXECUTE) {
                    jobRuns.add(run);
                    jobQueue.remove(run);
                    jobIDToJobRun.remove(run.getJobId());
                }
            } catch (HyracksException exception) {
                // The required capacity exceeds maximum capacity.
                List<Exception> exceptions = new ArrayList<>();
                exceptions.add(exception);
                try {
                    // Fails the job.
                    jobManager.prepareComplete(run, JobStatus.FAILURE_BEFORE_EXECUTION, exceptions);
                } catch (HyracksException e) {
                    LOGGER.log(Level.ERROR, e.getMessage(), e);
                }
            }
        }
        return jobRuns;
    }

    @Override
    public Collection<JobRun> jobs() {
        List<JobRun> jobs = new ArrayList<>();
        JobRun[] arr = (JobRun[]) jobQueue.toArray();
        Collections.addAll(jobs, arr);
        return jobs;
    }

    @Override
    public void clear() {
        jobQueue.clear();
    }

    @Override
    public void notifyJobFinished(JobRun run) {

    }

    @Override
    public String printQueueInfo() {
        return null;
    }

    @Override
    public void cancel(JobId jobId) {

    }

    @Override
    public int size() {
        return jobQueue.size();
    }

}