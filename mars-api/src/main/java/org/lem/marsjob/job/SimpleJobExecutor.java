package org.lem.marsjob.job;

import org.lem.marsjob.service.JobScheduleService;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SimpleJobExecutor implements Job {
    protected Logger LOG = LoggerFactory.getLogger(this.getClass());

    //TODO 暂时按FireTime+jobID，来判断是否重复任务，后续直接在调度层进行
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
        if (!jobScheduleService.scheduleJob(jobKey.getGroup() + "_" + jobKey.getName(), jobExecutionContext.getScheduledFireTime()))
            return;
        LOG.info("execute job:{},fire time:{}", jobKey, jobExecutionContext.getScheduledFireTime());
        internalJobExecute(jobExecutionContext);
    }

    protected abstract void internalJobExecute(JobExecutionContext jobExecutionContext) throws JobExecutionException;
}
