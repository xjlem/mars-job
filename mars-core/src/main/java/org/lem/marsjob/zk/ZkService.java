package org.lem.marsjob.zk;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.Watcher;
import org.lem.marsjob.enums.Operation;
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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;


public class ZkService {
    protected Logger LOG = LoggerFactory.getLogger(this.getClass());

    private Object lock = new Object();

    private String zkAddress;
    private String projectGroup;
    private Integer jobEventExpireDay = 7;
    private Integer jobExecuteLogExpireDay = 7;

    private EventHandler eventHandler = new DefaultHandler();


    private String zkJobPath;
    private String zkJobSynNodePath;
    private String zkAppNode;
    private String zkJobEventPath;

    private String nodeName;
    private String registPath;


    private volatile String synNodeVersion;
    private volatile String currentEventVersion;


    private ZkClient zkClient;

    private volatile boolean inited = false;

    private ConcurrentLinkedQueue<JobSynListener> listeners = new ConcurrentLinkedQueue();

    private ConcurrentLinkedQueue<RebalanceHandler> rebalanceHandlersListeners = new ConcurrentLinkedQueue();


    private Set<String> selfEvent = new HashSet<>();

    private static ThreadLocal<SimpleDateFormat> DAY_SDF =
            ThreadLocal.withInitial(()-> new SimpleDateFormat("yyyyMMdd"));

    private static ThreadLocal<SimpleDateFormat> MS_SDF = ThreadLocal.withInitial(()-> new SimpleDateFormat("yyyyMMddHHmmss"));

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
    }


    public ZkService() {

    }

    public ZkService(String zkAddress, String projectGroup, Integer jobEventExpireDay, Integer jobExecuteLogExpireDay) {
        this.zkAddress = zkAddress;
        this.projectGroup = projectGroup;
        this.jobEventExpireDay = jobEventExpireDay;
        this.jobExecuteLogExpireDay = jobExecuteLogExpireDay;
    }

    public ZkService(String zkAddress, String projectGroup) {
        this.zkAddress = zkAddress;
        this.projectGroup = projectGroup;
    }





    //TODO 会导致自己的update唤醒自己，下游listener要做事件去重,分片任务在连接状态为disconnect到expire期间可能出现任务丢失，可以任务扫描进行补偿（分片任务存在本身结构存在单点问题）
    public void init() {
        if (inited) return;
        inited = true;
        zkClient = new ZkClient(zkAddress, 30000, 5000, new ZkKryoSerialize());
        initPath();
        regist();
        currentEventVersion = getMax(zkClient.getChildren(zkJobEventPath));
        listeners.add(() ->
        {
            try {
                List<String> currentChilds = zkClient.getChildren(zkJobEventPath);
                String max = getMax(currentChilds);
                if (max == null || (currentEventVersion != null && max.compareTo(currentEventVersion) < 0)) return;
                List<String> unHandleEvents = getUnhandleEvent(currentEventVersion, currentChilds);
                currentEventVersion = max;
                handleEvents(unHandleEvents);
            } catch (Exception e) {
                LOG.error("同步任务出错", e);
            }
        });

        rebalanceHandlersListeners.add(()->{});

        zkClient.subscribeDataChanges(zkJobSynNodePath, new IZkDataListener() {
            @Override
            public void handleDataChange(String dataPath, Object data) throws Exception {
                String zkJobVersion = zkClient.readData(dataPath);
                synNodeVersion = zkJobVersion;
                listeners.forEach(e -> e.onTriggle());
            }

            @Override
            public void handleDataDeleted(String dataPath) throws Exception {

            }
        });

        zkClient.subscribeChildChanges(zkAppNode, (path,list)-> {
            rebalanceHandlersListeners.forEach(e->e.triggle());
        });

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

    private void initPath() {
        zkJobPath = PathUtil.getPath(PathUtil.MARS_JOB, projectGroup, "job");
        zkJobSynNodePath = PathUtil.getPath(PathUtil.MARS_JOB, projectGroup, "jobsyn");
        zkAppNode = PathUtil.getPath(PathUtil.MARS_JOB, projectGroup, "app");
        zkJobEventPath = PathUtil.getPath(PathUtil.MARS_JOB, projectGroup, "event");
        createPathIfAbsent(zkJobSynNodePath);
        createPathIfAbsent(zkJobEventPath);
        createPathIfAbsent(zkAppNode);
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
            eventHandler.handleEvent(param);
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


    public void clearOldZkValue() {
        LOG.info("clear old zk value");
        deleteExpireJobExecuteLog();
        deleteExpireJobEvent();
    }

    private void deleteExpireJobExecuteLog() {
        if (!zkClient.exists(zkJobPath)) return;
        List<String> children = zkClient.getChildren(zkJobPath);
        Calendar deadlineTime = Calendar.getInstance();
        deadlineTime.add(Calendar.DAY_OF_YEAR, -jobExecuteLogExpireDay);
        children = children.stream().filter(e -> {
            try {
                return DAY_SDF.get().parse(e).before(deadlineTime.getTime());
            } catch (ParseException e1) {
                return false;
            }
        }).map(e -> PathUtil.getPath(zkJobPath, e)).collect(Collectors.toList());
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


    public boolean scheduleJob(String jobIdentity, Date fireTime) {
        Preconditions.checkNotNull(jobIdentity);
        String path = PathUtil.getPath(zkJobPath, getTodayDate(), jobIdentity, MS_SDF.get().format(fireTime));
        return createPersistent(path);
    }

    public ShardingParams getShardingParams(String jobIdentity, Date fireTime) {
        Preconditions.checkNotNull(jobIdentity);
        String path = PathUtil.getPath(zkJobPath, getTodayDate(), jobIdentity, MS_SDF.get().format(fireTime));
        ShardingParams shardingParams;
        if (createPersistent(path)) {
            List<String> nodes = zkClient.getChildren(zkAppNode);
            int index = nodes.indexOf(nodeName);
            int total = nodes.size();
            shardingParams = new ShardingParams(index, total);
            zkClient.writeData(path, shardingParams);
        } else {
            shardingParams = zkClient.readData(path);
        }
        return shardingParams;
    }

    public ShardingParams getShardingParams() {
        List<String> nodes = zkClient.getChildren(zkAppNode);
        int index = nodes.indexOf(nodeName);
        int total = nodes.size();
        ShardingParams shardingParams = new ShardingParams(index, total);
        return shardingParams;
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

    public void unRegist() {
        if (registPath != null)
            zkClient.delete(registPath);
    }


    public interface JobSynListener {
        void onTriggle();
    }

    public interface EventHandler {
        void handleEvent(JobParam jobParam);
    }
    public interface RebalanceHandler{
        void triggle();
    }

    private class DefaultHandler implements EventHandler {

        @Override
        public void handleEvent(JobParam jobParam) {
            System.out.println("use defalut handler " + jobParam);
        }
    }

    public String getZkAddress() {
        return zkAddress;
    }

    public void setZkAddress(String zkAddress) {
        this.zkAddress = zkAddress;
    }

    public String getProjectGroup() {
        return projectGroup;
    }

    public void setProjectGroup(String projectGroup) {
        this.projectGroup = projectGroup;
    }

    public EventHandler getEventHandler() {
        return eventHandler;
    }

    public void setEventHandler(EventHandler eventHandler) {
        this.eventHandler = eventHandler;
    }

    public  void addRebalanceListener(RebalanceHandler rebalanceHandler){
        rebalanceHandlersListeners.add(rebalanceHandler);
    }
}
