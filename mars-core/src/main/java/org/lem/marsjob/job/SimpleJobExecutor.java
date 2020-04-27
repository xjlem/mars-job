package org.lem.marsjob.job;

import org.lem.marsjob.pojo.ExecuteJobParam;
import org.lem.marsjob.pojo.JobHandler;
import org.lem.marsjob.pojo.JobParam;
import org.lem.marsjob.service.JobSynService;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

public abstract class SimpleJobExecutor implements Job, JobHandler {
    protected Logger LOG = LoggerFactory.getLogger(this.getClass());

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        JobSynService jobSynService = null;
        try {
            SchedulerContext schedulerContext = jobExecutionContext.getScheduler().getContext();
            jobSynService = (JobSynService) schedulerContext.get(JobSynService.SCHEDULE_SERVICE_KEY);
        } catch (SchedulerException e) {
            throw new JobExecutionException(e);
        }
        JobParam jobParam= (JobParam) jobExecutionContext.getJobDetail().getJobDataMap().get("jobParam");
        ExecuteJobParam executeJobParam=new ExecuteJobParam(jobParam,jobExecutionContext.getFireTime());
        try {
            if (!jobSynService.scheduleJob(executeJobParam))
                return;
        } catch (Exception e) {
            LOG.error("",e);
            return;
        }
        LOG.info("execute job:{},fire time:{}", jobExecutionContext.getScheduledFireTime());
        try {
            Object context=  jobExecutionContext.getScheduler().getContext().get(JobSynService.CONTEXT_KEY);
            executeJobParam.setContext(context);
        } catch (SchedulerException e) {
            LOG.error("start work fail",e);
        }
        Exception exception=null;
        try{
            execute(executeJobParam);
        }catch (Exception e){
            exception=e;
        }
        jobSynService.jobCompleteCallBack(executeJobParam,exception);
    }

}
