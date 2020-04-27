package org.lem.marsjob.mq;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.concurrent.atomic.AtomicBoolean;

public class Master {
    private String zkAddress;
    private CuratorFramework curatorFramework;
    private AtomicBoolean master=new AtomicBoolean(false);
    private String projectName;


    public Master(String zkAddress, String projectName) {
        this.zkAddress = zkAddress;
        this.projectName=projectName;
    }

    public void init() throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        curatorFramework = CuratorFrameworkFactory.newClient(zkAddress, retryPolicy);
        curatorFramework.start();
        LeaderLatch latch=new LeaderLatch(curatorFramework,"/leader"+projectName);
        latch.addListener(new LeaderLatchListener() {
            @Override
            public void isLeader() {
                master.set(true);
            }

            @Override
            public void notLeader() {
                master.set(false);
            }
        });
        latch.start();
    }

    public boolean isMaster(){
        return master.get();
    }


    public static void main(String[] args) throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient("localhost:2181", retryPolicy);
        client.start();

       /* LeaderSelectorListener leaderSelectorListener=new LeaderSelectorListener() {
            @Override
            public void takeLeadership(CuratorFramework curatorFramework) throws Exception {
                System.out.println("master");
            }

            @Override
            public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
                System.out.println(connectionState);
            }
        };
        LeaderLatch latch=new LeaderLatch(client,"/leader");
       latch.addListener(new LeaderLatchListener() {
           @Override
           public void isLeader() {
               System.out.println("aaaaaaaaa");
           }

           @Override
           public void notLeader() {
               System.out.println("bbbbbbb");

           }
       });
        latch.start();*/
        System.in.read();
    }




}
