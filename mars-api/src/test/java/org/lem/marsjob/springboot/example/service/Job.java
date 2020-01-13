package org.lem.marsjob.springboot.example.service;

import org.lem.marsjob.job.SimpleJobExecutor;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class Job extends SimpleJobExecutor {
    @Override
    protected void internalJobExecute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        System.out.println("---------------");
        System.out.println("simple job");
        System.out.println("---------------");

    }
}
