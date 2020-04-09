package org.lem.marsjob.springboot.example.service;

import org.lem.marsjob.job.SimpleJobExecutor;
import org.lem.marsjob.pojo.ExecuteJobParam;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class Job extends SimpleJobExecutor {

    @Override
    public void execute(ExecuteJobParam executeJobParam) {
        System.out.println("---------------");
        System.out.println("simple job");
        System.out.println("---------------");
    }
}
