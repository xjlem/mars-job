package org.lem.marsjob.service;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
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
import org.springframework.scheduling.quartz.SchedulerFactoryBean;

import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class JobScheduleService implements InitializingBean {
    protected Logger LOG = LoggerFactory.getLogger(this.getClass());
    private final ZkService.EventHandler DEFAULT_HANDLER= new DefaultEventHandler();
    private final ZkService.RebalanceHandler DEFAULT_REBALANCE= new DefaultRebalance();

    public static final String SCHEDULE_SERVICE_KEY = "schedule_service_key";

    private ZkService zkService;

    private SchedulerFactoryBean schedulerFactoryBean;

    private Map<JobKey,JobParam> balanceJob=new ConcurrentHashMap<>();

    private Supplier<ShardingParams> shardingParams;

    private Scheduler scheduler;

    private volatile boolean inited = false;

    private ImmutableList<JobEvenetListener> listeners = ImmutableList.of();


    JobScheduleService(ZkService zkService, SchedulerFactoryBean schedulerFactoryBean, ZkService.EventHandler eventHandler, List<JobEvenetListener> listeners, List<ZkService.RebalanceHandler> rebalanceHandlers) {
        this.zkService = zkService;
        this.schedulerFactoryBean = schedulerFactoryBean;
        if(eventHandler==null)
            zkService.setEventHandler(DEFAULT_HANDLER);
        else
            zkService.setEventHandler(eventHandler);
        if(listeners!=null) {
            this.listeners = ImmutableList.copyOf(listeners);
        }
        if(rebalanceHandlers!=null&&rebalanceHandlers.size()>0) {
            rebalanceHandlers.forEach(e->zkService.addRebalanceListener(e));
        }else
            zkService.addRebalanceListener(DEFAULT_REBALANCE);
    }




    class DefaultRebalance implements ZkService.RebalanceHandler {
           @Override
           public void triggle() {
               ShardingParams old=shardingParams.get();
               shardingParams= Suppliers.memoize(()->zkService.getShardingParams());
               if(old.equals(shardingParams.get()))
                   return;
               balanceJob.keySet().forEach(jobKey -> {
                   try {
                       scheduler.deleteJob(jobKey);
                   } catch (SchedulerException e) {
                       LOG.error("",e);
                   }
               });
               balanceJob.values().forEach(jobParam -> {
                   try {
                       addJob(jobParam,false);
                   } catch (SchedulerException e) {
                            LOG.error("",e);
                   }
               });

       };
    }

    class DefaultEventHandler implements ZkService.EventHandler{
        @Override
        public void handleEvent(JobParam jobParam) {
            listeners.forEach(e->e.listen(jobParam));
            Operation operation = Operation.getOperationByCode(jobParam.getOperationCode());
            try {
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
                LOG.error("handle event error:", jobParam, e);
            }
        }

    }





    public void addJob(JobParam jobParam, boolean syn) throws SchedulerException {
        if (syn) {
            jobParam.setOperationCode(Operation.ADD.getOperationCode());
            sendOperation(jobParam);
        }
            innerAddJob(jobParam);


    }

    private boolean isSelfJob(JobParam jobParam) {
        if (jobParam.isBalance()){
            ShardingParams params = shardingParams.get();
            HashCode hashCode = Hashing.murmur3_32().hashString(jobParam.getJobGroup() + "_" + jobParam.getJobName(), Charset.defaultCharset());
            int index = Hashing.consistentHash(hashCode, params.getTotal());
            if (index != params.getIndex())
                return false;
        }
        return true;
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
        innerStopJob(jobParam);
        innerAddJob(jobParam);
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
            innerStopJob(jobParam);
        }
        CronTrigger trigger = getCronTrigger(jobParam, jobKey);
        if (jobParam.isBalance())
            balanceJob.put(jobKey, jobParam);
        if (isSelfJob(jobParam)) {
            scheduler.scheduleJob(jobDetail, trigger);
        }
    }

    private JobDetail getJobDetailByJobParam(JobParam jobParam) {
        JobDetail jobDetail = JobBuilder.newJob(jobParam.getJobClass()).withIdentity(jobParam.getJobName(), jobParam.getJobGroup()).build();
        if (jobParam.getJobData() != null)
            jobDetail.getJobDataMap().putAll(jobParam.getJobData());
        return jobDetail;
    }




    private void innerStopJob(JobParam jobParam) throws SchedulerException {
        JobKey jobKey = JobKey.jobKey(jobParam.getJobName(), jobParam.getJobGroup());
        scheduler.deleteJob(jobKey);
        if(jobParam.isBalance())
            balanceJob.remove(jobKey);
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
        shardingParams= Suppliers.memoize(()->zkService.getShardingParams());
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

    public ImmutableList<JobEvenetListener> getListeners() {
        return listeners;
    }

    public void setListeners(ImmutableList<JobEvenetListener> listeners) {
        this.listeners = listeners;
    }


}
