package org.lem.marsjob.service;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.lem.marsjob.pojo.ExecuteJobParam;
import org.lem.marsjob.pojo.JobParam;
import org.lem.marsjob.pojo.ShardingParams;
import org.quartz.SchedulerException;


public interface JobSynService {

    String SCHEDULE_SERVICE_KEY = "SCHEDULE_SERVICE_KEY";
    String CONTEXT_KEY = "CONTEXT_KEY";

    /**
     * 任务同步
     * @param jobParam
     */
    void sendOperation(JobParam jobParam) throws InterruptedException, RemotingException, MQClientException, MQBrokerException, Exception;


    /**
     * 任务调度
     * @param jobParam
     * @return
     */
    boolean scheduleJob(ExecuteJobParam jobParam) throws InterruptedException, RemotingException, MQClientException, MQBrokerException, Exception;


    /**
     * 定时任务的增删
     * @param jobParam
     * @throws SchedulerException
     */
    void handleJobParam(JobParam jobParam) throws SchedulerException;

    void jobCompleteCallBack(ExecuteJobParam executeJobParam,Exception exception);

    default void close(){};

    default void init() throws SchedulerException, MQClientException, Exception {};

    default void clean(){}

    ShardingParams getShardingParams();
}

