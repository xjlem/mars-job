package org.lem.marsjob.service;

import com.google.common.collect.ImmutableList;
import org.lem.marsjob.enums.Operation;
import org.lem.marsjob.pojo.JobParam;
import org.lem.marsjob.pojo.ShardingParams;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class JobScheduleService implements InitializingBean {
    protected Logger LOG = LoggerFactory.getLogger(this.getClass());
    private JobSynService jobSynService;
    private ImmutableList<JobEvenetListener> listeners = ImmutableList.of();

    public JobScheduleService() {

    }

    public JobScheduleService(JobSynService jobSynService, List<JobEvenetListener> listeners) {
        this.jobSynService = jobSynService;
        if(listeners!=null) {
            this.listeners = ImmutableList.copyOf(listeners);
        }

    }

    public void addJob(JobParam jobParam, boolean syn) throws Exception {
        jobParam.setOperationCode(Operation.ADD.getOperationCode());
        if (syn) {
            sendOperation(jobParam);
        }
        jobSynService.handleJobParam(jobParam);
    }

    public void deleteJob(JobParam jobParam, boolean syn) throws Exception {
        jobParam.setOperationCode(Operation.REMOVE.getOperationCode());
        if (syn) {
            sendOperation(jobParam);
        }
        jobSynService.handleJobParam(jobParam);
    }

    public void reScheduleJob(JobParam jobParam, boolean syn) throws Exception {
        jobParam.setOperationCode(Operation.UPDATE_CRON.getOperationCode());
        if (syn) {
            sendOperation(jobParam);
        }
        jobSynService.handleJobParam(jobParam);
    }


    private void sendOperation(JobParam jobParam) throws Exception {
        jobSynService.sendOperation(jobParam);
    }

    @Override
    public void afterPropertiesSet() throws SchedulerException {
    }


    public Set<String> getScheduleJobsDetail() throws SchedulerException {
        Set<String> allScheduledJobs = new HashSet<String>();
        /*Scheduler scheduler = getSchedulerFactoryBean().getScheduler();
        for (String groupName : scheduler.getJobGroupNames()) {

            for (JobKey jobKey : scheduler.getJobKeys(GroupMatcher.jobGroupEquals(groupName))) {

                String jobName = jobKey.getName();
                String jobGroup = jobKey.getGroup();

                //get job's trigger
                List<Trigger> triggers = (List<Trigger>) scheduler.getTriggersOfJob(jobKey);
                Date nextFireTime = triggers.get(0).getNextFireTime();

                allScheduledJobs.add("[jobName] : " + jobName + " [groupName] : "
                        + jobGroup + " - " + nextFireTime);
            }
        }*/
        return allScheduledJobs;
    }




    public ShardingParams getShardingParams() {
        return jobSynService.getShardingParams();
    }




    public ImmutableList<JobEvenetListener> getListeners() {
        return listeners;
    }

    public void setListeners(ImmutableList<JobEvenetListener> listeners) {
        this.listeners = listeners;
    }


}
