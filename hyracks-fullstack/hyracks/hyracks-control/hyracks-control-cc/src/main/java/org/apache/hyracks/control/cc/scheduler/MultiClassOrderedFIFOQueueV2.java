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

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.api.job.resource.IJobCapacityController;
import org.apache.hyracks.control.cc.job.IJobManager;
import org.apache.hyracks.control.cc.job.JobManagerWithUserPriority;
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
public class MultiClassOrderedFIFOQueueV2 implements IJobQueue {

    private final Logger LOGGER = LogManager.getLogger();

    private final Map<JobId, JobRun> memoryQueue = new LinkedHashMap<>();
    private final IJobManager jobManager;
    private final IJobCapacityController jobCapacityController;
    private final Map<JobId, JobSpecification.JobSizeTag> jobIdJobSizeTagMap = new HashMap<>();
    private final int jobQueueCapacity;
    private MPLQueue zeroShortQueue;
    private MPLQueue zeroLongQueue;
    private MPLQueue smallMPLQueue;
    private MPLQueue mediumMPLQueue;
    private MPLQueue largeMPLQueue;
    private double DevThreshold = 2;

    private String memoryQueueToString() {
        StringBuilder sb = new StringBuilder();
        for (JobRun run : memoryQueue.values()) {
            sb.append("JobID: " + run.getJobId() + "- USERID: " + run.getJobSpecification().getUserID() + ",");
        }
        return sb.toString();
    }

    public MultiClassOrderedFIFOQueueV2(IJobManager jobManager, IJobCapacityController jobCapacityController) {
        this.jobManager = jobManager;
        this.jobCapacityController = jobCapacityController;
        this.jobQueueCapacity = jobManager.getJobQueueCapacity();
        this.zeroShortQueue = new MPLQueue(JobSpecification.JobSizeTag.ZERO_SHORT, 1, 0.5);
        this.zeroLongQueue = new MPLQueue(JobSpecification.JobSizeTag.ZERO_LONG, 1, 160);
        this.smallMPLQueue = new MPLQueue(JobSpecification.JobSizeTag.SMALL, 1, 48.18);
        this.mediumMPLQueue = new MPLQueue(JobSpecification.JobSizeTag.MEDIUM, 1, 179.51);
        this.largeMPLQueue = new MPLQueue(JobSpecification.JobSizeTag.LARGE, 1, 327.07);
    }

    class MPLQueue {
        private final JobSpecification.JobSizeTag queueSizeTag;
        private final Map<JobId, JobRun> jobs = new LinkedHashMap<>();
        private long sumExecutionTimes = 0L;
        private long sumResponseTime = 0L;
        private int countOfExecutedJobs = 0;
        private int MPL;
        private int currentMPL = 0;
        private double candidateExecutionTime = 1;
        private long avgMemoryUsage = 0;

        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("queue_sizetag: " + this.queueSizeTag + ",");
            sb.append("queue_MPL: " + this.MPL + ",");
            sb.append("jobs:{ ");
            for (JobId jid : jobs.keySet()) {
                sb.append("jobID:" + jid + "--" + get(jid).getJobSpecification().getUserID() + ",");
            }
            sb.append("}\n");
            return sb.toString();
        }

        public MPLQueue(JobSpecification.JobSizeTag sizeTag, int MPL, double standaloneExecTime) {
            this.queueSizeTag = sizeTag;
            this.MPL = MPL;
            this.candidateExecutionTime = standaloneExecTime;
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

        public JobRun getHead() {
            if (jobs.values().iterator().hasNext())
                return jobs.values().iterator().next();
            return null;
        }

        public JobRun remove(JobId id) {
            LOGGER.warn("Removing JOBID: " + id + " from queue: " + queueSizeTag + ". Current jobs in this Q: "
                    + jobs.toString());
            return jobs.remove(id);
        }

        public JobRun get(JobId id) {
            LOGGER.warn("getting job id with id: " + id + " from queue with jobs of: " + jobs.toString());
            return jobs.get(id);
        }
    }

    private MPLQueue getQueue(JobSpecification.JobSizeTag sizeTag) {
        if (sizeTag == JobSpecification.JobSizeTag.ZERO_SHORT) {
            return zeroShortQueue;
        } else if (sizeTag == JobSpecification.JobSizeTag.ZERO_LONG) {
            return zeroLongQueue;
        } else if (sizeTag == JobSpecification.JobSizeTag.ZERO) {
            return zeroLongQueue;
        } else if (sizeTag == JobSpecification.JobSizeTag.SMALL) {
            return smallMPLQueue;
        } else if (sizeTag == JobSpecification.JobSizeTag.MEDIUM) {
            return mediumMPLQueue;
        } else if (sizeTag == JobSpecification.JobSizeTag.LARGE) {
            return largeMPLQueue;
        } else {
            throw new InvalidParameterException("Invalid SizeTag." + sizeTag);
        }
    }

    private void printAllStats() {
        StringBuilder sb = new StringBuilder();
        //activeRunMap
        LOGGER.warn("ActiveRunMap:" + ((JobManagerWithUserPriority) jobManager).printElementsInActiveRunMap());
        LOGGER.warn("MemoryQueue:" + memoryQueueToString());
        sb.append("ZeroShortMPLQueue:" + zeroShortQueue.toString() + "\n");
        sb.append("ZeroLongMPLQueue:" + zeroLongQueue.toString() + "\n");
        sb.append("SmallMPLQueue:" + smallMPLQueue.toString() + "\n");
        sb.append("MediumMPLQueue:" + mediumMPLQueue.toString() + "\n");
        sb.append("LargeMPLQueue:" + largeMPLQueue.toString() + "\n");
        LOGGER.warn(sb.toString());
    }

    @Override
    public void add(JobRun run) throws HyracksException {
        //JobManager adds the size tag. if query has size tag of ZERO, job manager runs it immediately without
        // adding it to any queue.
        JobSpecification.JobSizeTag sizeTag = run.getJobSpecification().getSizeTag();
        jobIdJobSizeTagMap.put(run.getJobId(), sizeTag);
        MPLQueue queue = getQueue(sizeTag);
        run.setAddedToQueueTime(System.nanoTime());
        queue.put(run.getJobId(), run);
        LOGGER.warn("Added JOBID " + run.getJobId() + "With USERID" + run.getJobSpecification().getUserID()
                + " with the " + "size of " + run.getJobSpecification().getSizeTag()
                + " to its MPL Queue. Current MPL: " + queue.currentMPL + " queue size" + queue.jobs.size());
        printAllStats();
        while (queue.currentMPL < queue.MPL) {
            int ret = removeMPLHeadAndAddItToMemoryQueue(queue);
            printAllStats();
            if (ret < 0) {
                break;
            }
        }

    }

    @Override
    public JobRun remove(JobId jobId) {
        JobSpecification.JobSizeTag sizeTag = jobIdJobSizeTagMap.get(jobId);
        jobIdJobSizeTagMap.remove(jobId);

        if (memoryQueue.containsKey(jobId)) {
            JobRun run = memoryQueue.remove(jobId);
            LOGGER.warn("removeFrom MemoryQueue: Removing JOBID " + run.getJobId() + "With USERID"
                    + run.getJobSpecification().getUserID() + " with the " + "size of "
                    + run.getJobSpecification().getSizeTag());
            printAllStats();
            return run;
        }
        MPLQueue queue = getQueue(sizeTag);
        return queue.remove(jobId);
    }

    @Override
    public JobRun get(JobId jobId) {
        JobSpecification.JobSizeTag sizeTag = jobIdJobSizeTagMap.get(jobId);
        if (sizeTag == null) {
            LOGGER.warn("SizeTag is null?");
            return null;
        }
        if (memoryQueue.containsKey(jobId)) {
            JobRun run = memoryQueue.get(jobId);
            LOGGER.warn("Get is called in Dewitt Scheduler for JOBID " + run.getJobId() + "With USERID"
                    + run.getJobSpecification().getUserID() + " with the " + "size of "
                    + run.getJobSpecification().getSizeTag() + " from memoryQueue");//here
            printAllStats();
            return run;
        }
        MPLQueue queue = getQueue(sizeTag);
        JobRun run = queue.get(jobId);
        LOGGER.warn("Get is called in Dewitt Scheduler for JOBID " + run.getJobId() + "With USERID"
                + run.getJobSpecification().getUserID() + " with the " + "size of "
                + run.getJobSpecification().getSizeTag() + " from MPLQueue");//here
        printAllStats();
        return run;
    }

    @Override
    public List<JobRun> pull() {
        List<JobRun> jobRuns = new ArrayList<>();
        //Although only one thread is adding and removing queries to/from the memoryQueue, it is better to work on a
        // copy of memoryQueue in case later on it becomes multi-threaded, so no adding and removing to the same
        // queue happens concurrently which can put the queue in a wrong state(specially if we are iterating it at the
        // same time)
        Map<JobId, JobRun> memoryQueueCopy = new HashMap<>();
        boolean noMoreCapacity = false;
        do {
            Iterator<JobRun> runIterator = memoryQueue.values().iterator();
            LOGGER.warn("In pull: memoryQueueElements " + memoryQueueToString());
            while (runIterator.hasNext()) {
                JobRun run = runIterator.next();
                JobSpecification job = run.getJobSpecification();
                LOGGER.warn("Pull: First Job in the Q: JOBID: " + run.getJobId() + "With USERID"
                        + run.getJobSpecification().getUserID() + " with the " + "size of "
                        + run.getJobSpecification().getSizeTag());
                // Cluster maximum capacity can change over time, thus we have to re-check if the job should be rejected
                // or not.
                try {
                    IJobCapacityController.JobSubmissionStatus status =
                            jobCapacityController.allocate(job, run.getJobId(), run.getFlags());
                    // Checks if the job can be executed immediately.
                    if (status == IJobCapacityController.JobSubmissionStatus.EXECUTE) {
                        jobRuns.add(run);
                        runIterator.remove(); // Removes the selected job.
                        LOGGER.warn("Pull: memoryQ after remove: " + memoryQueueToString());
                    } else {
                        LOGGER.warn("No more capacity? No pull!");
                        noMoreCapacity = true;
                        break;
                    }
                } catch (HyracksException exception) {
                    // The required capacity exceeds maximum capacity.
                    List<Exception> exceptions = new ArrayList<>();
                    exceptions.add(exception);
                    runIterator.remove(); // Removes the job from the queue.
                    try {
                        // Fails the job.
                        jobManager.prepareComplete(run, JobStatus.FAILURE_BEFORE_EXECUTION, exceptions);
                    } catch (HyracksException e) {
                        LOGGER.log(Level.ERROR, e.getMessage(), e);
                    }
                }
            }
        } while (!noMoreCapacity && memoryQueue.size() > 0);
        LOGGER.warn("End OF Pull:");
        printAllStats();
        return jobRuns;
    }

    @Override
    public Collection<JobRun> jobs() {
        List<JobRun> allJobs = new ArrayList<>();
        allJobs.addAll(memoryQueue.values());
        allJobs.addAll(zeroShortQueue.jobs.values());
        allJobs.addAll(zeroLongQueue.jobs.values());
        allJobs.addAll(smallMPLQueue.jobs.values());
        allJobs.addAll(mediumMPLQueue.jobs.values());
        allJobs.addAll(largeMPLQueue.jobs.values());
        return Collections.unmodifiableCollection(allJobs);
    }

    @Override
    public void clear() {
        memoryQueue.clear();
        zeroShortQueue.jobs.clear();
        zeroLongQueue.jobs.clear();
        smallMPLQueue.jobs.clear();
        mediumMPLQueue.jobs.clear();
        largeMPLQueue.jobs.clear();
    }

    @Override
    public void notifyJobFinished(JobRun run) {
        JobSpecification.JobSizeTag sizeTag = run.getJobSpecification().getSizeTag();
        LOGGER.warn("Job Finished: JOBID " + run.getJobId() + "With USERID" + run.getJobSpecification().getUserID()
                + " " + "with the " + "size of " + run.getJobSpecification().getSizeTag() + " is finished. Size tag: "
                + sizeTag);
        //get executionTime
        long executionTime = run.getEndTime() - run.getStartTime();
        long queueTime = run.getAddedToQueueTime() - run.getStartTime();
        MPLQueue queue = getQueue(sizeTag);
        queue.currentMPL--;
        queue.sumExecutionTimes += executionTime;
        queue.sumResponseTime += queueTime;
        queue.countOfExecutedJobs++;
        LOGGER.warn("Queue with size: " + queue.queueSizeTag + " just finished the job with JOBID " + run.getJobId()
                + "With USERID" + run.getJobSpecification().getUserID() + " with the " + "size of "
                + run.getJobSpecification().getSizeTag() + " currentMPL: " + queue.currentMPL + " queue MPL: "
                + queue.MPL);
        printAllStats();
        if (sizeTag == JobSpecification.JobSizeTag.SMALL) {
            LOGGER.warn("Calling to check the fairness...");
            LOGGER.warn("Before Fairness: " + "ZeroShortMPL:" + zeroShortQueue.MPL + "ZeroLongMPL: " + zeroLongQueue.MPL
                    + "SmallMPL: " + smallMPLQueue.MPL + " mediumMPL: " + mediumMPLQueue.MPL + " LargeMPL: "
                    + largeMPLQueue.MPL);
            checkFairnessAndUpdateMPL();
            LOGGER.warn("After Fairness: " + "ZeroShortMPL:" + zeroShortQueue.MPL + "ZeroLongMPL: " + zeroLongQueue.MPL
                    + "SmallMPL: " + smallMPLQueue.MPL + " mediumMPL: " + mediumMPLQueue.MPL + " LargeMPL: "
                    + largeMPLQueue.MPL);
            updateMPLQueueAfterFairness(zeroShortQueue);
            updateMPLQueueAfterFairness(zeroLongQueue);
            updateMPLQueueAfterFairness(smallMPLQueue);
            updateMPLQueueAfterFairness(mediumMPLQueue);
            updateMPLQueueAfterFairness(largeMPLQueue);
        } else {
            while (queue.currentMPL < queue.MPL) {
                if (removeMPLHeadAndAddItToMemoryQueue(queue) < 0)
                    break;
            }
        }
    }

    private void updateMPLQueueAfterFairness(MPLQueue queue) {
        int oldMPL = queue.currentMPL;
        while (queue.currentMPL < queue.MPL) {
            if (removeMPLHeadAndAddItToMemoryQueue(queue) < 0) {
                break;
            }
        }
        LOGGER.warn("updated current MPL for class with sizetag of " + queue.queueSizeTag + " from " + oldMPL + " to "
                + queue.currentMPL);
    }

    private int removeMPLHeadAndAddItToMemoryQueue(MPLQueue queue) {
        if (queue.getHead() != null) {
            JobRun head = queue.getHead();
            head.setAddedToMemoryQueueTime(System.nanoTime());
            memoryQueue.put(head.getJobId(), head);
            LOGGER.warn("removeMPLHeadAndAddItToMemoryQueue: Removing JOBID " + head.getJobId() + "With USERID"
                    + head.getJobSpecification().getUserID() + " with the " + "size of "
                    + head.getJobSpecification().getSizeTag() + " from queue with size " + queue.queueSizeTag + " and"
                    + " adding it to memory queue. memory queue now contains: " + memoryQueueToString());
            queue.remove(head.getJobId());
            queue.currentMPL++;
            return 0;
        }
        return -1;
    }

    @Override
    public String printQueueInfo() {
        StringBuilder sb = new StringBuilder();
        for (JobSpecification.JobSizeTag tag : JobSpecification.JobSizeTag.values()) {
            MPLQueue queue = getQueue(tag);
            sb.append(queue.toString() + "\n");
        }
        return sb.toString();
    }

    @Override
    public void cancel(JobId jobId) {
        JobSpecification.JobSizeTag sizeTag = jobIdJobSizeTagMap.get(jobId);
        MPLQueue queue = getQueue(sizeTag);
        queue.currentMPL--;
    }

    @Override
    public int size() {
        return jobIdJobSizeTagMap.size();
    }

    private void checkFairnessAndUpdateMPL() {
        //calculate the average response times for each class
        double avgExecTimeForZeroShortClass = zeroShortQueue.countOfExecutedJobs > 0
                ? (double) zeroShortQueue.sumExecutionTimes / zeroShortQueue.countOfExecutedJobs : 0;
        double avgRespTimeForZeroShortClass = zeroShortQueue.countOfExecutedJobs > 0
                ? (double) zeroShortQueue.sumResponseTime / zeroShortQueue.countOfExecutedJobs : 0;
        double avgExecTimeForZeroLongClass = zeroLongQueue.countOfExecutedJobs > 0
                ? (double) zeroLongQueue.sumExecutionTimes / zeroLongQueue.countOfExecutedJobs : 0;
        double avgRespTimeForZeroLongClass = zeroLongQueue.countOfExecutedJobs > 0
                ? (double) zeroLongQueue.sumResponseTime / zeroLongQueue.countOfExecutedJobs : 0;
        double avgExecTimeForSmallClass = smallMPLQueue.countOfExecutedJobs > 0
                ? (double) smallMPLQueue.sumExecutionTimes / smallMPLQueue.countOfExecutedJobs : 0;
        double avgRespTimeForSmallClass = smallMPLQueue.countOfExecutedJobs > 0
                ? (double) smallMPLQueue.sumResponseTime / smallMPLQueue.countOfExecutedJobs : 0;
        double avgExecTimeForMediumClass = mediumMPLQueue.countOfExecutedJobs > 0
                ? (double) mediumMPLQueue.sumExecutionTimes / mediumMPLQueue.countOfExecutedJobs : 0;
        double avgRespTimeForMediumClass = mediumMPLQueue.countOfExecutedJobs > 0
                ? (double) mediumMPLQueue.sumResponseTime / mediumMPLQueue.countOfExecutedJobs : 0;
        double avgExecTimeForLargeClass = largeMPLQueue.countOfExecutedJobs > 0
                ? (double) largeMPLQueue.sumExecutionTimes / largeMPLQueue.countOfExecutedJobs : 0;
        double avgRespTimeForLargeClass = largeMPLQueue.countOfExecutedJobs > 0
                ? (double) largeMPLQueue.sumResponseTime / largeMPLQueue.countOfExecutedJobs : 0;
        //Calculate Fairness Metric(Dev) and mean Response Time Change(MRT)
        //mean Response Time Change(MRT)
        double ZSP = (double) (avgRespTimeForZeroShortClass) / avgExecTimeForZeroShortClass * 100;
        double ZLP = (double) (avgRespTimeForZeroLongClass) / avgExecTimeForZeroLongClass * 100;
        double SP = (double) (avgRespTimeForSmallClass) / avgExecTimeForSmallClass * 100;
        double MP = (double) (avgRespTimeForMediumClass) / avgExecTimeForMediumClass * 100;
        double LP = (double) (avgRespTimeForLargeClass) / avgExecTimeForLargeClass * 100;
        List<Double> l = new LinkedList<>();
        l.add(ZSP);
        l.add(ZLP);
        l.add(SP);
        l.add(MP);
        l.add(LP);
        double avgP = 0;
        int count = 0;
        for (Double el : l) {
            if (el > 0) {
                avgP += el;
                count++;
            }
        }
        avgP = avgP / count;
        double Dev = 0;
        for (Double el : l) {
            if (el > 0) {
                Dev += Math.pow((el - avgP), 2);
            }
        }
        Dev = Math.sqrt(Dev);

        Double maxDiff = l.stream().mapToDouble(v -> v).max().orElseThrow();
        if (Dev > DevThreshold) {
            if (jobCapacityController.getNumberOfAvailableCores() == 0) {
                if (zeroLongQueue.MPL > 1) {
                    zeroLongQueue.MPL--;
                } else if (zeroShortQueue.MPL > 1) {
                    zeroShortQueue.MPL--;
                }
            } else if (maxDiff == ZSP) {
                zeroShortQueue.MPL++;
            } else if (maxDiff == ZLP) {
                zeroLongQueue.MPL++;
            } else if (SP > MP && MP > LP) {
                LOGGER.warn("Rule1");
                if (jobCapacityController.hasEnoughMemory(smallMPLQueue.avgMemoryUsage)) {
                    smallMPLQueue.MPL++;
                } else if (largeMPLQueue.MPL > 1) {
                    largeMPLQueue.MPL--;
                } else if (mediumMPLQueue.MPL > 1) {
                    mediumMPLQueue.MPL--;
                }
            } else if (MP > SP && SP > LP) {
                LOGGER.warn("Rule2");
                if (jobCapacityController.hasEnoughMemory(mediumMPLQueue.avgMemoryUsage)) {
                    mediumMPLQueue.MPL++;
                } else if (largeMPLQueue.MPL > 1) {
                    largeMPLQueue.MPL--;
                } else if (smallMPLQueue.MPL > 1) {
                    smallMPLQueue.MPL--;
                }
            } else if (LP > SP && SP > MP) {
                LOGGER.warn("Rule3");
                if (mediumMPLQueue.MPL > 1) {
                    mediumMPLQueue.MPL--;
                } else if (jobCapacityController.hasEnoughMemory(largeMPLQueue.avgMemoryUsage)) {
                    largeMPLQueue.MPL++;
                } else if (smallMPLQueue.MPL > 1) {
                    smallMPLQueue.MPL--;
                }
            } else if (LP > MP && MP > SP) {
                LOGGER.warn("Rule4");
                if (smallMPLQueue.MPL > 1) {
                    smallMPLQueue.MPL--;
                } else if (jobCapacityController.hasEnoughMemory(largeMPLQueue.avgMemoryUsage)) {
                    largeMPLQueue.MPL++;
                } else if (mediumMPLQueue.MPL > 1) {
                    mediumMPLQueue.MPL--;
                }
            } else if (SP > LP && LP > MP) {
                LOGGER.warn("Rule5");
                if (jobCapacityController.hasEnoughMemory(smallMPLQueue.avgMemoryUsage)) {
                    smallMPLQueue.MPL++;
                } else if (mediumMPLQueue.MPL > 1) {
                    mediumMPLQueue.MPL--;
                } else if (largeMPLQueue.MPL > 1) {
                    largeMPLQueue.MPL--;
                }
            } else if (MP > LP && LP > SP) {
                LOGGER.warn("Rule6");
                if (smallMPLQueue.MPL > 1) {
                    smallMPLQueue.MPL--;
                } else if (jobCapacityController.hasEnoughMemory(mediumMPLQueue.avgMemoryUsage)) {
                    mediumMPLQueue.MPL++;
                } else if (largeMPLQueue.MPL > 1) {
                    largeMPLQueue.MPL--;
                }
            }
        }
    }
}
