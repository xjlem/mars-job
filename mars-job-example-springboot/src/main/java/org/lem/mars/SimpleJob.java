package org.lem.mars;

import org.lem.marsjob.job.SimpleJobExecutor;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;

public class SimpleJob extends SimpleJobExecutor {
    @Override
    protected void internalJobExecute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        System.out.println("---------------------------");
        JobKey jobKey = jobExecutionContext.getJobDetail().getKey();
        System.out.println("do simple job, group:" + jobKey.getGroup() + " name:" + jobKey.getName());
        System.out.println("---------------------------");

    }
}
