package org.lem.marsjob.service;

import org.lem.marsjob.enums.Operation;
import org.lem.marsjob.job.DelegateSimpleJob;
import org.lem.marsjob.pojo.JobParam;
import org.lem.marsjob.pojo.ShardingParams;
import org.lem.marsjob.zk.ZkService;
import org.quartz.*;
import org.quartz.impl.matchers.GroupMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;

import java.util.*;

public class JobScheduleService implements InitializingBean {
    protected Logger LOG = LoggerFactory.getLogger(this.getClass());

    public static final String SCHEDULE_SERVICE_KEY = "schedule_service_key";
    @Autowired
    private ZkService zkService;
    @Autowired
    private SchedulerFactoryBean schedulerFactoryBean;

    private Scheduler scheduler;
    private volatile boolean inited = false;
    private List<JobEvenetListener> listeners=new ArrayList<>();


    public List<JobEvenetListener> getListeners() {
        return listeners;
    }

    public void setListeners(List<JobEvenetListener> listeners) {
        this.listeners = listeners;
    }

    public JobScheduleService(ZkService zkService, SchedulerFactoryBean schedulerFactoryBean) {
        this.zkService = zkService;
        this.schedulerFactoryBean = schedulerFactoryBean;
        zkService.setEventHandler(eventHandler());
    }

    public ZkService.EventHandler eventHandler() {
        ZkService.EventHandler eventHandler = new ZkService.EventHandler() {
            @Override
            public void handleEvent(JobParam jobParam) {
                Operation operation = Operation.getOperationByCode(jobParam.getOperationCode());
                try {
                    switch (operation) {
                        case REMOVE:
                            innerStopJob(jobParam);
                            break;
                        case UPDATE_CRON:
                            innerReScheduleJob(jobParam);
                            break;
                        case ADD:
                            innerAddJob(jobParam);
                            break;
                    }
                } catch (Exception e) {
                    LOG.error("handle event error:", jobParam, e);
                }
            }
        };
        return eventHandler;
    }


    public void addJob(JobParam jobParam, boolean syn) throws SchedulerException {
        if (syn) {
            jobParam.setOperationCode(Operation.ADD.getOperationCode());
            sendOperation(jobParam);
        }
        innerAddJob(jobParam);
    }

    public void deleteJob(JobParam jobParam, boolean syn) throws SchedulerException {
        if (syn) {
            jobParam.setOperationCode(Operation.REMOVE.getOperationCode());
            sendOperation(jobParam);
        }
        innerStopJob(jobParam);
    }

    public void reScheduleJob(JobParam jobParam, boolean syn) throws SchedulerException {
        if (syn) {
            jobParam.setOperationCode(Operation.UPDATE_CRON.getOperationCode());
            sendOperation(jobParam);
        }
        innerReScheduleJob(jobParam);
    }


    private void sendOperation(JobParam jobParam) {
        zkService.sendOperation(jobParam);
        zkService.updateJobSynNode();
    }


    private void innerAddJob(JobParam jobParam) throws SchedulerException {
        if (jobParam.getEndTime() != null && jobParam.getEndTime().before(new Date()))
            return;
        JobDetail jobDetail = getJobDetailByJobParam(jobParam);
        JobKey jobKey = jobDetail.getKey();
        if (scheduler.checkExists(jobKey)) {
            innerReScheduleJob(jobParam);
            return;
        }
        CronTrigger trigger = getCronTrigger(jobParam, jobKey);
        scheduler.scheduleJob(jobDetail, trigger);
    }

    private JobDetail getJobDetailByJobParam(JobParam jobParam) {
        JobDetail jobDetail = JobBuilder.newJob(jobParam.getJobClass()).withIdentity(jobParam.getJobName(), jobParam.getJobGroup()).build();
        if (jobParam.getJobData() != null)
            jobDetail.getJobDataMap().putAll(jobParam.getJobData());
        return jobDetail;
    }


    private void innerReScheduleJob(JobParam jobParam) throws SchedulerException {
        JobDetail jobDetail = getJobDetailByJobParam(jobParam);
        JobKey jobKey = jobDetail.getKey();
        scheduler.deleteJob(jobKey);
        innerAddJob(jobParam);
    }

    private void innerStopJob(JobParam jobParam) throws SchedulerException {
        JobKey jobKey = JobKey.jobKey(jobParam.getJobName(), jobParam.getJobGroup());
        scheduler.deleteJob(jobKey);
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

    public JobScheduleService() {

    }

    public JobScheduleService(ZkService zkService) {
        this.zkService = zkService;
    }


    @Override
    public void afterPropertiesSet() throws SchedulerException {
        init();
        scheduleClearOldZk();
    }

    public void init() throws SchedulerException {
        if (inited) return;
        inited = true;
        scheduler = schedulerFactoryBean.getScheduler();
        scheduler.getContext().put(SCHEDULE_SERVICE_KEY, this);
    }

    /**
     * 代理类清除历史数据
     */
    private void scheduleClearOldZk() {
        try {
            JobDetail jobDetail = JobBuilder.newJob(DelegateSimpleJob.class).
                    withIdentity("clearOldZk", "mars").build();
            jobDetail.getJobDataMap().put("class", ZkService.class.getName());
            jobDetail.getJobDataMap().put("method", "clearOldZkValue");
            JobKey jobKey = jobDetail.getKey();
            TriggerKey triggerKey = new TriggerKey(jobKey.getName(), jobKey.getGroup());
            TriggerBuilder<Trigger> builder = TriggerBuilder.newTrigger();
            CronTrigger trigger = builder.withIdentity(triggerKey)
                    .withSchedule(CronScheduleBuilder.cronSchedule("0 0 0 * * ?"))
                    .build();
            scheduler.scheduleJob(jobDetail, trigger);
        } catch (Exception e) {
            LOG.error("", e);
        }
    }


    public Set<String> getScheduleJobsDetail() throws SchedulerException {
        Set<String> allScheduledJobs = new HashSet<String>();
        Scheduler scheduler = getSchedulerFactoryBean().getScheduler();
        for (String groupName : scheduler.getJobGroupNames()) {

            for (JobKey jobKey : scheduler.getJobKeys(GroupMatcher.jobGroupEquals(groupName))) {

                String jobName = jobKey.getName();
                String jobGroup = jobKey.getGroup();

                //get job's trigger
                List<Trigger> triggers = (List<Trigger>) scheduler.getTriggersOfJob(jobKey);
                Date nextFireTime = triggers.get(0).getNextFireTime();

                allScheduledJobs.add("[jobName] : " + jobName + " [groupName] : "
                        + jobGroup + " - " + nextFireTime);
            }
        }
        return allScheduledJobs;
    }


    public boolean scheduleJob(String jobIdentity, Date fireTime) {
        return zkService.scheduleJob(jobIdentity, fireTime);
    }

    public ShardingParams getShardingParams(String jobIdentity, Date fireTime) {
        return zkService.getShardingParams(jobIdentity, fireTime);
    }


    public SchedulerFactoryBean getSchedulerFactoryBean() {
        return schedulerFactoryBean;
    }

    public void setSchedulerFactoryBean(SchedulerFactoryBean schedulerFactoryBean) {
        this.schedulerFactoryBean = schedulerFactoryBean;
    }

    public ZkService getZkService() {
        return zkService;
    }

    public void setZkService(ZkService zkService) {
        this.zkService = zkService;
    }


}
