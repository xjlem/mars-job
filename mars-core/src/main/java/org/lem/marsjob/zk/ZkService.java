package org.lem.marsjob.zk;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.zookeeper.Watcher;
import org.lem.marsjob.enums.Operation;
import org.lem.marsjob.job.DelegateSimpleJob;
import org.lem.marsjob.pojo.ExecuteJobParam;
import org.lem.marsjob.pojo.JobHandler;
import org.lem.marsjob.pojo.JobParam;
import org.lem.marsjob.pojo.ShardingParams;
import org.lem.marsjob.serialize.ZkKryoSerialize;
import org.lem.marsjob.service.JobSynService;
import org.lem.marsjob.util.KryoUtil;
import org.lem.marsjob.util.PathUtil;
import org.lem.marsjob.util.Utils;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;

import java.nio.charset.Charset;
import java.nio.file.Path;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;


public class ZkService extends Thread implements JobSynService , ApplicationContextAware {
    protected Logger LOG = LoggerFactory.getLogger(this.getClass());


    private Object lock = new Object();

    private ApplicationContext applicationContext;

    private String zkAddress;
    private String projectGroup;
    private Integer jobEventExpireDay = 7;
    private Integer jobExecuteLogExpireDay = 7;

    private final ZkService.RebalanceHandler DEFAULT_REBALANCE= new DefaultRebalance();

    //任务执行节点
    private String zkJobHistoryPath;
    //唤醒任务同步状态节点
    private String zkJobSynNodePath;
    //运行的机器节点
    private String zkAppNode;
    //同步节点
    private String zkJobEventPath;
    //任务监控
    private String zkJobMonitorPath;
    //正在运行的任务
    private String zkRunningJobPath;


    private String nodeName;
    private String registPath;

    private volatile String synNodeVersion;
    private volatile String currentEventVersion;

    private ZkClient zkClient;

    private volatile boolean inited = false;

    private volatile boolean stop = false;

    private JobSynListener jobSynlistener=new JobSynListener();

    private ConcurrentLinkedQueue<RebalanceHandler> rebalanceHandlersListeners = new ConcurrentLinkedQueue();

    private Set<String> selfEvent = new HashSet<>();

    private static ThreadLocal<SimpleDateFormat> DAY_SDF =
            ThreadLocal.withInitial(()-> new SimpleDateFormat("yyyyMMdd"));

    private static ThreadLocal<SimpleDateFormat> MS_SDF = ThreadLocal.withInitial(()-> new SimpleDateFormat("yyyyMMddHHmmss"));

    private LinkedBlockingQueue<ZkServiceEvent> zkEventQueue =new LinkedBlockingQueue<>();

    private ScheduledExecutorService scheduledExecutorService= Executors.newSingleThreadScheduledExecutor();

    private ExecutorService executorService= new ThreadPoolExecutor(10,10,0,TimeUnit.SECONDS,new ArrayBlockingQueue<>(100));

    private Supplier<ShardingParams> shardingParams;

    private Map<JobKey,JobParam> balanceJob=new ConcurrentHashMap<>();

    private SchedulerFactoryBean schedulerFactoryBean;

    private Scheduler scheduler;

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


    public void sendOperation(JobParam jobParam) {
        if (jobParam.getOperationCode() == -1)
            throw new IllegalArgumentException("job operation is not init");
        if (Strings.isNullOrEmpty(jobParam.getJobName()))
            throw new IllegalArgumentException("job name can not be null or empty");
        if (jobParam.getJobClass() == null && jobParam.getOperationCode() != Operation.REMOVE.getOperationCode())
            throw new IllegalArgumentException("job class can not be null");
        synchronized (lock) {
            String eventPath = zkClient.createPersistentSequential(zkJobEventPath + "/", jobParam);
            selfEvent.add(eventPath.substring(eventPath.lastIndexOf("/") + 1));
        }
        updateJobSynNode();
    }




    public ZkService() {

    }

    public ZkService(String zkAddress, String projectGroup, Integer jobEventExpireDay, Integer jobExecuteLogExpireDay,SchedulerFactoryBean schedulerFactoryBean) {
        this.zkAddress = zkAddress;
        this.projectGroup = projectGroup;
        this.jobEventExpireDay = jobEventExpireDay;
        this.jobExecuteLogExpireDay = jobExecuteLogExpireDay;
        this.schedulerFactoryBean=schedulerFactoryBean;
    }

    public ZkService(String zkAddress, String projectGroup) {
        this.zkAddress = zkAddress;
        this.projectGroup = projectGroup;
    }





    //TODO 会导致自己的update唤醒自己，下游listener要做事件去重,分片任务在连接状态为disconnect到expire期间可能出现任务丢失，可以任务扫描进行补偿（分片任务存在本身结构存在单点问题）
    @Override
    public void init() {
        if (inited) return;
        inited = true;
        zkClient = new ZkClient(zkAddress, 30000, 5000, new ZkKryoSerialize());
        initPath();
        regist();
        currentEventVersion = getMax(zkClient.getChildren(zkJobEventPath));
        scheduler = schedulerFactoryBean.getScheduler();
        try {
            scheduler.getContext().put(SCHEDULE_SERVICE_KEY, this);
            scheduler.getContext().put(CONTEXT_KEY,applicationContext);
        } catch (SchedulerException e) {
            e.printStackTrace();
        }
        shardingParams= Suppliers.memoize(()->getShardingParams());
        addZkListener();
        scheduledExecutorService.scheduleAtFixedRate(()->{
            zkEventQueue.add(ZkServiceEvent.REBALANCE);
        },20,20, TimeUnit.SECONDS);
        scheduledExecutorService.scheduleAtFixedRate(()->{
            zkEventQueue.add(ZkServiceEvent.SYN_JOB);
        },15,20, TimeUnit.SECONDS);
        scheduledExecutorService.scheduleAtFixedRate(()->{
            zkEventQueue.add(ZkServiceEvent.CHECK_JOB);
        },1,1, TimeUnit.MINUTES);

        this.start();
        rebalanceHandlersListeners.add(DEFAULT_REBALANCE);
        scheduleClean();
    }

    private void addZkListener(){
        //job syn watcher
        zkClient.subscribeDataChanges(zkJobSynNodePath, new IZkDataListener() {
            @Override
            public void handleDataChange(String dataPath, Object data) throws Exception {
                String zkJobVersion = zkClient.readData(dataPath);
                synNodeVersion = zkJobVersion;
                zkEventQueue.add(ZkServiceEvent.SYN_JOB);
            }
            @Override
            public void handleDataDeleted(String dataPath) throws Exception {

            }
        });

        zkClient.subscribeChildChanges(zkAppNode, (path,list)-> {
            zkEventQueue.add(ZkServiceEvent.REBALANCE);
        });

        //reregist
        zkClient.subscribeStateChanges(new IZkStateListener() {
            @Override
            public void handleStateChanged(Watcher.Event.KeeperState state) throws Exception {
            }

            @Override
            public void handleNewSession() throws Exception {
                regist();
            }

            @Override
            public void handleSessionEstablishmentError(Throwable error) throws Exception {

            }
        });
    }

    @Override
    public boolean scheduleJob(ExecuteJobParam executeJobParam) {
        Preconditions.checkNotNull(executeJobParam);
        String jobRunningPath = PathUtil.getPath(zkRunningJobPath, executeJobParam.getJobIdentity()+"_"+ MS_SDF.get().format(executeJobParam.getFireTime()));
        boolean scheduled= createPersistent(jobRunningPath,executeJobParam);
        if(scheduled){
            String monitorPath= PathUtil.getPath(zkJobMonitorPath,  executeJobParam.getJobIdentity()+"_"+ MS_SDF.get().format(executeJobParam.getFireTime()));
            scheduled=createEphemeral(monitorPath);
            if(scheduled){
                String jobHistoryPath = PathUtil.getPath(zkJobHistoryPath, getTodayDate(), executeJobParam.getJobIdentity(), MS_SDF.get().format(executeJobParam.getFireTime()));
                createPersistent(jobHistoryPath);
            }

        }
        return scheduled;
    }

    @Override
    public void jobCompleteCallBack(ExecuteJobParam executeJobParam, Exception exception) {
        String jobRunningPath = PathUtil.getPath(zkRunningJobPath, executeJobParam.getJobIdentity()+"_"+ MS_SDF.get().format(executeJobParam.getFireTime()));
        zkClient.delete(jobRunningPath);
        String monitorPath= PathUtil.getPath(zkJobMonitorPath,  executeJobParam.getJobIdentity()+"_"+ MS_SDF.get().format(executeJobParam.getFireTime()));
        zkClient.delete(monitorPath);
    }


    @Override
    public void handleJobParam(JobParam jobParam) {
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



    @Override
    public ShardingParams getShardingParams() {
        List<String> nodes = zkClient.getChildren(zkAppNode);
        int index = nodes.indexOf(nodeName);
        int total = nodes.size();
        ShardingParams shardingParams = new ShardingParams(index, total);
        return shardingParams;
    }

    @Override
    public void close(){
        stop=true;
        this.interrupt();
        unRegist();
    }

    private void unRegist() {
        if (registPath != null)
            zkClient.delete(registPath);
    }

    @Override
    public void run() {
        while (!stop){
            try {
                ZkServiceEvent event=zkEventQueue.take();
                switch (event){
                    case SYN_JOB:
                        jobSynlistener.onTriggle();
                        break;
                    case REBALANCE:
                        rebalanceHandlersListeners.forEach(e->e.triggle());
                        break;
                    case CHECK_JOB:
                        checkJob();
                    default:break;
                }
            } catch (InterruptedException e) {
                LOG.warn("",e);
            }
        }
    }

    private void checkJob() {
        List<String> runningJobs=zkClient.getChildren(zkRunningJobPath);
        List<String> lostJobs=new LinkedList<>();
        if(!runningJobs.isEmpty()){
            for(String job :runningJobs){
                String monitorPath= PathUtil.getPath(zkJobMonitorPath,job);
                if(!zkClient.exists(monitorPath)){
                    lostJobs.add(job);
                }
            }
        }

        for(String lostJob:lostJobs){
            executorService.execute(()->{
                String runningPath= PathUtil.getPath(zkRunningJobPath,lostJob);
                if(zkClient.exists(runningPath)){
                    ExecuteJobParam executeJobParam=zkClient.readData(runningPath);
                    String monitorPath= PathUtil.getPath(zkJobMonitorPath,  lostJob);
                    if(createEphemeral(monitorPath)){
                        executeJobParam.setContext(applicationContext);
                        try {
                            JobHandler jobHandler = executeJobParam.getJobHandler().newInstance();
                            jobHandler.execute(executeJobParam);
                        } catch (Exception e) {
                            LOG.error("execute job fail,will retry");
                        }
                        zkClient.delete(monitorPath);
                        zkClient.delete(runningPath);
                    }

                }
            });
        }
    }

    private void initPath() {
        zkJobHistoryPath = PathUtil.getPath(PathUtil.MARS_JOB, projectGroup, "job");
        zkJobSynNodePath = PathUtil.getPath(PathUtil.MARS_JOB, projectGroup, "jobsyn");
        zkAppNode = PathUtil.getPath(PathUtil.MARS_JOB, projectGroup, "app");
        zkJobEventPath = PathUtil.getPath(PathUtil.MARS_JOB, projectGroup, "event");
        zkJobMonitorPath = PathUtil.getPath(PathUtil.MARS_JOB, projectGroup, "jobMonitor");
        zkRunningJobPath = PathUtil.getPath(PathUtil.MARS_JOB, projectGroup, "jobRunning");
        createPathIfAbsent(zkJobHistoryPath);
        createPathIfAbsent(zkJobSynNodePath);
        createPathIfAbsent(zkAppNode);
        createPathIfAbsent(zkJobEventPath);
        createPathIfAbsent(zkJobMonitorPath);
        createPathIfAbsent(zkRunningJobPath);

    }

    private void createPathIfAbsent(String zkJobSynNodePath) {
        if (!zkClient.exists(zkJobSynNodePath))
            zkClient.createPersistent(zkJobSynNodePath, true);
    }

    private void handleEvents(List<String> unHandleEventsPath) throws SchedulerException {
        for (String path : unHandleEventsPath) {
            synchronized (lock) {
                if (selfEvent.contains(path)) continue;
            }
            String completePath = zkJobEventPath + "/" + path;
            JobParam param = zkClient.readData(completePath);
            LOG.info("receive event{}", param);
            Operation operation = Operation.getOperationByCode(param.getOperationCode());
            if (operation == null)
                throw new IllegalArgumentException("CAN NOT PROCESS OPERATION: CODE " + param.getOperationCode());
            handleJobParam(param);
        }
    }


    private List<String> getUnhandleEvent(String currentVersion, List<String> events) {
        if (currentVersion == null) return events;
        Collections.sort(events);
        int side = -1;
        for (int i = 0; i < events.size(); i++) {
            if (events.get(i).compareTo(currentVersion) > 0) {
                side = i;
                break;
            }
        }
        if (side == -1) return new ArrayList<>();
        return new ArrayList<>(events.subList(side, events.size()));
    }

    private String getMax(List<String> events) {
        String max = null;
        if (events != null && events.size() > 0) {
            Collections.sort(events);
            max = events.get(events.size() - 1);
        }
        if (currentEventVersion == null)
            return max;
        if (max != null && max.compareTo(currentEventVersion) > 0)
            return max;
        else
            return currentEventVersion;
    }

    @Override
    public void clean() {
        LOG.info("clear old zk value");
        deleteExpireJobExecuteLog();
        deleteExpireJobEvent();
    }

    private void deleteExpireJobExecuteLog() {
        if (!zkClient.exists(zkJobHistoryPath)) return;
        List<String> children = zkClient.getChildren(zkJobHistoryPath);
        Calendar deadlineTime = Calendar.getInstance();
        deadlineTime.add(Calendar.DAY_OF_YEAR, -jobExecuteLogExpireDay);
        children = children.stream().filter(e -> {
            try {
                return DAY_SDF.get().parse(e).before(deadlineTime.getTime());
            } catch (ParseException e1) {
                return false;
            }
        }).map(e -> PathUtil.getPath(zkJobHistoryPath, e)).collect(Collectors.toList());
        children.forEach(e -> zkClient.deleteRecursive(e));
    }

    private void deleteExpireJobEvent() {
        List<String> events = zkClient.getChildren(zkJobEventPath);
        for (String event : events) {
            long time = zkClient.getCreationTime(zkJobEventPath + "/" + event);
            if (System.currentTimeMillis() - time > jobEventExpireDay * 24 * 60 * 60 * 1000) {
                zkClient.delete(PathUtil.getPath(zkJobEventPath, event));
                selfEvent.remove(event);
            }
        }
    }

    private boolean createEphemeral(String path) {
        if (zkClient.exists(path)) return false;
        try {
            createPathIfAbsent(parentPath(path));
            zkClient.createEphemeral(path);
        } catch (Exception e) {
            return false;
        }
        return true;
    }
    private boolean createPersistent(String path,Object data) {
        if (zkClient.exists(path)) return false;
        try {
            createPathIfAbsent(parentPath(path));
            zkClient.createPersistent(path, data);
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    private boolean createPersistent(String path) {
        if (zkClient.exists(path)) return false;
        try {
            createPathIfAbsent(parentPath(path));
            zkClient.createPersistent(path, Utils.getLocalHost());
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    private String parentPath(String path) {
        return path.substring(0, path.lastIndexOf("/"));
    }

    private String getTodayDate() {
        return DAY_SDF.get().format(new Date());
    }

    public void updateJobSynNode() {
        synNodeVersion = UUID.randomUUID().toString();
        zkClient.writeData(zkJobSynNodePath, synNodeVersion);
    }

    public void regist() {
        String nodeName = Utils.getLocalHost() + MS_SDF.get().format(new Date());
        this.nodeName = nodeName;
        String path = PathUtil.getPath(zkAppNode, nodeName);
        if (registPath != null)
            zkClient.delete(registPath);
        zkClient.createEphemeral(path, "mars-regist");
        registPath = path;
    }

    public List<String> getRegistPath() {
        return zkClient.getChildren(zkAppNode);
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


    /**
     * 代理类清除历史数据
     */
    private void scheduleClean() {
        try {
            JobDetail jobDetail = JobBuilder.newJob(DelegateSimpleJob.class).
                    withIdentity("clean", "mars").build();
            jobDetail.getJobDataMap().put("class", JobSynService.class.getName());
            jobDetail.getJobDataMap().put("method", "clean");
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



    private void innerStopJob(JobParam jobParam) throws SchedulerException {
        JobKey jobKey = JobKey.jobKey(jobParam.getJobName(), jobParam.getJobGroup());
        scheduler.deleteJob(jobKey);
        if(jobParam.isBalance())
            balanceJob.remove(jobKey);
    }

    private JobDetail getJobDetailByJobParam(JobParam jobParam) {
        JobDetail jobDetail = JobBuilder.newJob(jobParam.getJobClass()).withIdentity(jobParam.getJobName(), jobParam.getJobGroup()).build();
        jobDetail.getJobDataMap().put("jobParam",jobParam);
        return jobDetail;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext=applicationContext;
    }


    private class JobSynListener {
        void onTriggle(){
            try {
                List<String> currentChilds = zkClient.getChildren(zkJobEventPath);
                String max = getMax(currentChilds);
                if (max == null || (currentEventVersion != null && max.compareTo(currentEventVersion) <= 0)) return;
                List<String> unHandleEvents = getUnhandleEvent(currentEventVersion, currentChilds);
                currentEventVersion = max;
                handleEvents(unHandleEvents);
            } catch (Exception e) {
                LOG.error("同步任务出错", e);
            }
        }
    }

    public interface RebalanceHandler{
        void triggle();
    }

    class DefaultRebalance implements ZkService.RebalanceHandler {
        @Override
        public void triggle() {
            ShardingParams old=shardingParams.get();
            shardingParams= Suppliers.memoize(()->getShardingParams());
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
                    innerAddJob(jobParam);
                } catch (Exception e) {
                    LOG.error("",e);
                }
            });

        };
    }


    enum  ZkServiceEvent{
        REBALANCE,SYN_JOB, CHECK_JOB ;
    }
}

