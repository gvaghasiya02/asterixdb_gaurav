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
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.api.job.resource.IJobCapacityController;
import org.apache.hyracks.control.cc.job.IJobManager;
import org.apache.hyracks.control.cc.job.JobRun;
import org.apache.hyracks.util.annotations.GuardedBy;
import org.apache.hyracks.util.annotations.NotThreadSafe;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * An implementation of IJobQueue that gives more priority to jobs that are submitted earlier.
 */
@NotThreadSafe
@GuardedBy("JobManager")
public class Colorado_V2 implements IJobQueue {

    private final Logger LOGGER = LogManager.getLogger();

    private final Map<JobId, JobRun> memoryQueue = new LinkedHashMap<>();
    private final IJobManager jobManager;
    private final IJobCapacityController jobCapacityController;
    private final Map<JobId, MPLQueue> jobIdToQueueMap = new HashMap<>();
    private final int jobQueueCapacity;
    private ArrayList<MPLQueue> queues = new ArrayList<>();
    private int numberOfQueues = 11;
    private BitSet queueHasAnyJob = new BitSet(numberOfQueues);

    public Colorado_V2(IJobManager jobManager, IJobCapacityController jobCapacityController) {
        this.jobManager = jobManager;
        this.jobCapacityController = jobCapacityController;
        this.jobQueueCapacity = jobManager.getJobQueueCapacity();
        double[] candidateExecTimes =
                new double[] { 0.5, 55.5, 27.33, 41.4, 58.55, 76.27, 124.3, 160.46, 215.23, 295.49, 337.32 };
        for (int i = 0; i < numberOfQueues; i++) {
            queues.add(new MPLQueue(i, candidateExecTimes[i]));
        }
    }

    class MPLQueue {
        private final Map<JobId, JobRun> jobs = new LinkedHashMap<>();
        private double topQuerySlowDown;
        private long sumExecutionTimesIncludingQueueTime = 0L;
        private int countOfExecutedJobs = 0;
        private Iterator<Map.Entry<JobId, JobRun>> it;
        private double candidateQueryExecTime = 1;
        private int id;

        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("queue_id: " + id + ",\n");
            sb.append("jobs:{ ");
            for (JobId jid : jobs.keySet()) {
                sb.append(jid + ",");
            }
            sb.append("}");
            return sb.toString();
        }

        public MPLQueue(int id, double candidateQueryExecTime) {
            this.id = id;
            this.candidateQueryExecTime = candidateQueryExecTime;
        }

        public int getQueueSize() {
            return jobs.size();
        }

        public void put(JobId id, JobRun run) throws HyracksException {
            if (getQueueSize() >= jobQueueCapacity) {
                throw HyracksException.create(ErrorCode.JOB_QUEUE_FULL, jobQueueCapacity);
            }
            this.jobs.put(id, run);
        }

        public JobRun remove(JobId id) {
            return jobs.remove(id);
        }

        public JobRun get(JobId id) {
            return jobs.get(id);
        }

        public JobRun getFirst() {
            it = jobs.entrySet().iterator();
            if (it.hasNext()) {
                return it.next().getValue();
            }
            return null;
        }
    }

    private void calculateSlowDown(MPLQueue queue, long now) {
        JobRun nextJob = queue.getFirst();
        if (nextJob != null) {
            long createTime = nextJob.getCreateTime();
            long waitTime = (now - createTime);
            if (queue.countOfExecutedJobs > 0) {
                long avgExecTime = queue.sumExecutionTimesIncludingQueueTime / queue.countOfExecutedJobs;
                queue.topQuerySlowDown = (double) (waitTime + avgExecTime) / avgExecTime;
            } else
                queue.topQuerySlowDown =
                        (double) (waitTime + queue.candidateQueryExecTime) / queue.candidateQueryExecTime;
        } else {
            queue.topQuerySlowDown = -1;
        }

    }

    private MPLQueue getQueue(JobRun run) {
        if (run.getJobSpecification().getSizeTag() == JobSpecification.JobSizeTag.ZERO_SHORT) {
            return queues.get(0);
        } else {
            double ratio = jobCapacityController.getMemoryRatio(run.getJobSpecification());
            if (ratio <= 0.05) {
                return queues.get(1);
            } else if (ratio <= 0.10) {
                return queues.get(2);
            } else if (ratio <= 0.15) {
                return queues.get(3);
            } else if (ratio <= 0.20) {
                return queues.get(4);
            } else if (ratio <= 0.25) {
                return queues.get(5);
            } else if (ratio <= 0.40) {
                return queues.get(6);
            } else if (ratio <= 0.55) {
                return queues.get(7);
            } else if (ratio <= 0.70) {
                return queues.get(8);
            } else if (ratio <= 0.85) {
                return queues.get(9);
            }
            return queues.get(10);
        }
    }

    @Override
    public void add(JobRun run) throws HyracksException {
        //JobManager adds the size tag. if query has size tag of ZERO, job manager runs it immediately without
        // adding it to any queue.
        MPLQueue queue = getQueue(run);
        //Make sure ZERO is handled out of queue.
        run.setAddedToQueueTime(System.nanoTime());
        queue.put(run.getJobId(), run);
        jobIdToQueueMap.put(run.getJobId(), queue);
        queueHasAnyJob.set(queue.id);
        //pick jobs

    }

    @Override
    public JobRun remove(JobId jobId) {
        MPLQueue queue = jobIdToQueueMap.get(jobId);
        JobRun ret = queue.remove(jobId);
        if (ret != null && queue.getFirst() == null) {
            queueHasAnyJob.set(queue.id, false);
        }
        return ret;
    }

    @Override
    public JobRun get(JobId jobId) {
        MPLQueue queue = jobIdToQueueMap.get(jobId);
        return queue.get(jobId);
    }

    @Override
    public List<JobRun> pull() {
        List<JobRun> jobRuns = new ArrayList<>();
        boolean canExecute = true;
        double maxSlowDown = -1;
        while (canExecute) {
            MPLQueue nextJobQueue = null;
            long now = System.currentTimeMillis();
            for (int i = queueHasAnyJob.nextSetBit(0); i >= 0 && i < queueHasAnyJob.size(); i =
                    queueHasAnyJob.nextSetBit(i + 1)) {
                if (i >= 0) {
                    MPLQueue queue = queues.get(i);
                    calculateSlowDown(queue, now);
                    if (queue.topQuerySlowDown > maxSlowDown) {
                        maxSlowDown = queue.topQuerySlowDown;
                        nextJobQueue = queue;
                    }
                }
            }
            if (nextJobQueue != null) {
                JobRun nextToRun = nextJobQueue.getFirst();
                try {
                    IJobCapacityController.JobSubmissionStatus status = jobCapacityController
                            .allocate(nextToRun.getJobSpecification(), nextToRun.getJobId(), nextToRun.getFlags());
                    // Checks if the job can be executed immediately.
                    if (status == IJobCapacityController.JobSubmissionStatus.EXECUTE) {
                        jobRuns.add(nextToRun);
                        remove(nextToRun.getJobId());
                        //calculateSlowDown(nextJobQueue, now);
                    } else {
                        canExecute = false;
                    }
                } catch (HyracksException exception) {
                    // The required capacity exceeds maximum capacity.
                    List<Exception> exceptions = new ArrayList<>();
                    exceptions.add(exception);
                    //                    nextJobQueue.remove(nextToRun.getJobId()); // Removes the job from the queue.
                    try {
                        // Fails the job.
                        jobManager.prepareComplete(nextToRun, JobStatus.FAILURE_BEFORE_EXECUTION, exceptions);
                        remove(nextToRun.getJobId()); // Removes the job from the queue.
                    } catch (HyracksException e) {
                        LOGGER.log(Level.ERROR, e.getMessage(), e);
                    }
                }
            } else {
                canExecute = false;
            }
        }
        return jobRuns;
    }

    @Override
    public Collection<JobRun> jobs() {
        List<JobRun> allJobs = new ArrayList<>();
        for (MPLQueue q : queues) {
            allJobs.addAll(q.jobs.values());
        }
        return Collections.unmodifiableCollection(allJobs);
    }

    @Override
    public void clear() {
        for (MPLQueue q : queues) {
            q.jobs.clear();
        }
    }

    @Override
    public void notifyJobFinished(JobRun run) {
        LOGGER.warn(run.toJSON_Shortened());
        LOGGER.info(run.toJSON());
        MPLQueue queue = jobIdToQueueMap.get(run.getJobId());
        queue.countOfExecutedJobs++;
        long executionTime = run.getEndTime() - run.getStartTime();
        queue.sumExecutionTimesIncludingQueueTime += executionTime;

    }

    @Override
    public String printQueueInfo() {
        StringBuilder sb = new StringBuilder();
        sb.append("memory queue: {");
        for (JobId jid : memoryQueue.keySet()) {
            sb.append(jid + ",");
        }
        sb.append("},\nMPL Queues:{\n");

        for (MPLQueue queue : queues) {
            sb.append(queue.toString() + "\n");
        }
        sb.append("}\n");
        return sb.toString();
    }

    @Override
    public void cancel(JobId jobId) {

    }

    @Override
    public int size() {
        return jobIdToQueueMap.size();
    }

}
