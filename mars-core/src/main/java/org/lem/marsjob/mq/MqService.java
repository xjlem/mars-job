package org.lem.marsjob.mq;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.lem.marsjob.enums.Operation;
import org.lem.marsjob.pojo.ExecuteJobParam;
import org.lem.marsjob.pojo.JobHandler;
import org.lem.marsjob.pojo.JobParam;
import org.lem.marsjob.pojo.ShardingParams;
import org.lem.marsjob.service.JobSynService;
import org.lem.marsjob.util.KryoUtil;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;

import java.util.Date;
import java.util.List;

public class MqService implements JobSynService, ApplicationContextAware {
    protected Logger LOG = LoggerFactory.getLogger(this.getClass());

    public static final String JOB_PARAM = "jobParam";
    private DefaultMQPushConsumer jobExecuteConsumer;
    private DefaultMQPushConsumer jobSynConsumer;

    private DefaultMQProducer producer;
    private String jobSynTopic;
    private String jobExecuteTopic;
    private Master master;
    private Object context;
    private String nameserver;
    private String projectGroup;
    private String zkAddress;
    private long jobMaxExecuteTime=-1;

    private SchedulerFactoryBean schedulerFactoryBean;

    private Scheduler scheduler;


    public MqService(String zkAddress,String nameserver, String projectGroup, SchedulerFactoryBean schedulerFactoryBean) {
        this.zkAddress=zkAddress;
        this.nameserver = nameserver;
        this.projectGroup = projectGroup;
        this.schedulerFactoryBean=schedulerFactoryBean;
        jobExecuteTopic="mars_execute_"+projectGroup;
        jobSynTopic="mars_syn_"+projectGroup;
    }

    @Override
    public void init() throws Exception {
        scheduler=schedulerFactoryBean.getScheduler();
        scheduler.getContext().put(SCHEDULE_SERVICE_KEY, this);
        scheduler.getContext().put(CONTEXT_KEY,context);
        jobExecuteConsumer =new DefaultMQPushConsumer();
        jobExecuteConsumer.setNamesrvAddr(nameserver);
        jobExecuteConsumer.subscribe(jobExecuteTopic,"*");
        jobExecuteConsumer.setConsumerGroup(projectGroup);
        if(jobMaxExecuteTime!=-1)
            jobExecuteConsumer.setConsumeTimeout(jobMaxExecuteTime);

        //jobExecuteConsumer.setAdjustThreadPoolNumsThreshold();


        jobExecuteConsumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                for (MessageExt msg : msgs) {
                    ExecuteJobParam executeJob = KryoUtil.readClassAndObject(msg.getBody());
                    executeJob.setContext(context);
                    try {
                        JobHandler jobHandler = executeJob.getJobHandler().newInstance();
                        jobHandler.execute(executeJob);
                    } catch (Exception e) {
                        LOG.error("execute job fail,will retry",e);
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        jobSynConsumer =new DefaultMQPushConsumer();
        jobSynConsumer.setInstanceName("jobSyn"+ UtilAll.getPid());
        jobSynConsumer.setNamesrvAddr(nameserver);
        jobSynConsumer.subscribe(jobSynTopic,"*");
        jobSynConsumer.setConsumerGroup(projectGroup);
        jobSynConsumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                for (MessageExt msg : msgs) {
                    JobParam jobParam = KryoUtil.readClassAndObject(msg.getBody());
                    try {
                        handleJobParam(jobParam);
                    } catch (SchedulerException e) {
                        LOG.error("syn error",e);
                    }
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }


        });
        master=new Master(zkAddress,projectGroup);
        producer=new DefaultMQProducer();
        producer.setProducerGroup(projectGroup);
        producer.setNamesrvAddr(nameserver);
        producer.start();
        master.init();
        jobExecuteConsumer.start();
        jobSynConsumer.start();
    }

    @Override
    public void handleJobParam(JobParam jobParam) throws SchedulerException {
        Operation operation = Operation.getOperationByCode(jobParam.getOperationCode());
        try {
            assert operation != null;
            switch (operation) {
                case REMOVE:
                    innerStopJob(jobParam);
                    break;
                case UPDATE_CRON:
                    innerStopJob(jobParam);
                    innerAddJob(jobParam);
                    break;
                case ADD:
                    innerAddJob(jobParam);
                    break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void jobCompleteCallBack(ExecuteJobParam executeJobParam, Exception exception) {

    }


    private void innerAddJob(JobParam jobParam) throws SchedulerException {
        if (jobParam.getEndTime() != null && jobParam.getEndTime().before(new Date()))
            return;
        JobDetail jobDetail = getJobDetailByJobParam(jobParam);
        JobKey jobKey = jobDetail.getKey();
        if (scheduler.checkExists(jobKey)) {
            innerStopJob(jobParam);
        }
        CronTrigger trigger = getCronTrigger(jobParam, jobKey);
        scheduler.scheduleJob(jobDetail, trigger);
    }

    private void innerStopJob(JobParam jobParam) throws SchedulerException {
        JobKey jobKey = JobKey.jobKey(jobParam.getJobName(), jobParam.getJobGroup());
        scheduler.deleteJob(jobKey);
    }

    @Override
    public void sendOperation(JobParam jobParam) throws Exception {
        Message message = new Message(jobSynTopic, KryoUtil.writeClassAndObject(jobParam));
        producer.send(message, new MessageQueueSelector() {
            @Override
            public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                return mqs.get(0);
            }
        }, null);
    }


    @Override
    public boolean scheduleJob(ExecuteJobParam jobParam) throws Exception {
        Message message = new Message(jobExecuteTopic, KryoUtil.writeClassAndObject(jobParam));
        if (master.isMaster()) {
            producer.send(message);
            LOG.info("schedule job:"+message);
        }
        return false;
    }


    private JobDetail getJobDetailByJobParam(JobParam jobParam) {
        JobDetail jobDetail = JobBuilder.newJob(jobParam.getJobClass()).withIdentity(jobParam.getJobName(), jobParam.getJobGroup()).build();
        jobDetail.getJobDataMap().put(JOB_PARAM,jobParam);
        return jobDetail;
    }

    private CronTrigger getCronTrigger(JobParam jobParam, JobKey jobKey) {
        TriggerKey triggerKey = new TriggerKey(jobKey.getName(), jobKey.getGroup());
        TriggerBuilder<Trigger> builder = TriggerBuilder.newTrigger();
        if (jobParam.getStartTime() != null)
            builder.startAt(System.currentTimeMillis() < jobParam.getStartTime().getTime() ? jobParam.getStartTime() : new Date());
        if (jobParam.getEndTime() != null)
            builder.endAt(jobParam.getEndTime());
        return builder.withIdentity(triggerKey)
                .withSchedule(CronScheduleBuilder.cronSchedule(jobParam.getCron()))
                .build();
    }

    @Override
    public void close() {
        jobExecuteConsumer.shutdown();
    }

    @Override
    public void clean() {

    }

    @Override
    public ShardingParams getShardingParams() {
        //consumer.
        return null;
    }


    public long getJobMaxExecuteTime() {
        return jobMaxExecuteTime;
    }

    public void setJobMaxExecuteTime(long jobMaxExecuteTime) {
        this.jobMaxExecuteTime = jobMaxExecuteTime;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
            this.context=applicationContext;
    }
}
