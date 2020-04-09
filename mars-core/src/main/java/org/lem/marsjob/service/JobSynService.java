package org.lem.marsjob.service;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.zookeeper.Watcher;
import org.lem.marsjob.EventHandler;
import org.lem.marsjob.enums.Operation;
import org.lem.marsjob.pojo.ExecuteJobParam;
import org.lem.marsjob.pojo.JobParam;
import org.lem.marsjob.pojo.ShardingParams;
import org.lem.marsjob.serialize.ZkKryoSerialize;
import org.lem.marsjob.util.PathUtil;
import org.lem.marsjob.util.Utils;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;


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

    default void close(){};

    default void init() throws SchedulerException, MQClientException, Exception {};

    default void clean(){}

    ShardingParams getShardingParams();
}

