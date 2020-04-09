package org.lem.marsjob.mq;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.lem.marsjob.enums.Operation;
import org.lem.marsjob.pojo.ExecuteJobParam;
import org.lem.marsjob.pojo.JobHandler;
import org.lem.marsjob.pojo.JobParam;
import org.lem.marsjob.pojo.ShardingParams;
import org.lem.marsjob.service.JobParamService;
import org.lem.marsjob.util.KryoUtil;
import org.lem.marsjob.service.JobSynService;
import org.quartz.*;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;

import java.util.Date;
import java.util.List;

public class MqService implements JobSynService, ApplicationContextAware {
    public static final String JOB_PARAM = "jobParam";
    private DefaultMQPushConsumer consumer;
    private DefaultMQProducer producer;
    private String jobSyntopic;
    private String jobExecuteTopic;
    private Master master;
    private Object context;
    private String nameserver;
    private String projectGroup;
    private String zkAddress;

    private JobParamService jobParamService;

    private SchedulerFactoryBean schedulerFactoryBean;

    private Scheduler scheduler;

    public MqService(String zkAddress,String nameserver, String projectGroup, SchedulerFactoryBean schedulerFactoryBean) {
        this.zkAddress=zkAddress;
        this.nameserver = nameserver;
        this.projectGroup = projectGroup;
        this.schedulerFactoryBean=schedulerFactoryBean;
        jobExecuteTopic="mars_"+projectGroup;
    }

    @Override
    public void init() throws Exception {
        scheduler=schedulerFactoryBean.getScheduler();
        scheduler.getContext().put(SCHEDULE_SERVICE_KEY, this);
        scheduler.getContext().put(CONTEXT_KEY,context);
        consumer=new DefaultMQPushConsumer();
        consumer.setNamesrvAddr(nameserver);
        consumer.subscribe(jobExecuteTopic,"*");
        consumer.setConsumerGroup(projectGroup);
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                for (MessageExt msg : msgs) {
                    ExecuteJobParam executeJob = KryoUtil.readClassAndObject(msg.getBody());
                    executeJob.setContext(context);
                    try {
                        JobHandler jobHandler = executeJob.getJobHandler().newInstance();
                        jobHandler.execute(executeJob);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        master=new Master(zkAddress,projectGroup);
        producer=new DefaultMQProducer();
        producer.setProducerGroup(projectGroup);
        producer.setNamesrvAddr(nameserver);
        producer.start();
        master.init();
        consumer.start();
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
        Message message = new Message(jobSyntopic, KryoUtil.writeClassAndObject(jobParam));
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
        consumer.shutdown();
    }

    @Override
    public void clean() {

    }

    @Override
    public ShardingParams getShardingParams() {
        //consumer.
        return null;
    }




    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
            this.context=applicationContext;
    }
}
