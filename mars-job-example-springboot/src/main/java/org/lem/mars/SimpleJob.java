package org.lem.mars;

import org.lem.marsjob.job.SimpleJobExecutor;
import org.lem.marsjob.pojo.ExecuteJobParam;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;

public class SimpleJob extends SimpleJobExecutor {
    protected void internalJobExecute(JobExecutionContext jobExecutionContext) throws JobExecutionException {


    }

    @Override
    public void execute(ExecuteJobParam executeJobParam) {
        System.out.println("---------------------------");
       String key = executeJobParam.getJobIdentity();
        System.out.println("do simple job, group:" + key);
        System.out.println("---------------------------");
    }
}
