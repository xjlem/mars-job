package org.lem.marsjob.job;

import org.lem.marsjob.pojo.ShardingParams;
import org.lem.marsjob.service.JobScheduleService;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ShardingJobExecutor implements Job {
    protected Logger LOG = LoggerFactory.getLogger(this.getClass());

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        JobScheduleService jobScheduleService = null;
        try {
            SchedulerContext schedulerContext = jobExecutionContext.getScheduler().getContext();
            jobScheduleService = (JobScheduleService) schedulerContext.get(JobScheduleService.SCHEDULE_SERVICE_KEY);
        } catch (SchedulerException e) {
            throw new JobExecutionException(e);
        }
        JobKey jobKey = jobExecutionContext.getJobDetail().getKey();
        ShardingParams shardingParams = jobScheduleService.getShardingParams(jobKey.getGroup() + "_" + jobKey.getName(), jobExecutionContext.getScheduledFireTime());
        LOG.info("execute job:{},fire time:{},params:{}", jobKey, jobExecutionContext.getScheduledFireTime(), shardingParams);
        internalJobExecute(jobExecutionContext, shardingParams);
    }

    protected abstract void internalJobExecute(JobExecutionContext jobExecutionContext, ShardingParams params) throws JobExecutionException;
}
