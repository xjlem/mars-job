package org.lem.mars;

import org.lem.marsjob.job.ShardingJobExecutor;
import org.lem.marsjob.pojo.ShardingParams;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class ShardingJob extends ShardingJobExecutor {
    @Override
    protected void internalJobExecute(JobExecutionContext jobExecutionContext, ShardingParams params) throws JobExecutionException {
        System.out.println("---------------------------");
        System.out.println("do sharding job");
        System.out.println("sharding params:" + params);
        System.out.println("---------------------------");
    }
}
