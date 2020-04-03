package org.lem.marsjob.service;

import com.google.common.base.Preconditions;
import org.lem.marsjob.zk.ZkService;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;

import java.util.ArrayList;
import java.util.List;

public class JobScheduleServiceFactory {
    private List<JobEvenetListener> evenetListeners=new ArrayList<>();

    private ZkService.EventHandler eventHandler;

    private List<ZkService.RebalanceHandler> rebalanceListeners=new ArrayList<>();

    private ZkService zkService;

    private SchedulerFactoryBean schedulerFactoryBean;

    public JobScheduleServiceFactory(ZkService zkService, SchedulerFactoryBean schedulerFactoryBean) {
        this.zkService = zkService;
        this.schedulerFactoryBean = schedulerFactoryBean;
    }

    public List<JobEvenetListener> getEvenetListeners() {
        return evenetListeners;
    }

    public void setEvenetListeners(List<JobEvenetListener> evenetListeners) {
        this.evenetListeners = evenetListeners;
    }

    public JobScheduleService build(){
        Preconditions.checkNotNull(zkService);
        Preconditions.checkNotNull(schedulerFactoryBean);
        return new  JobScheduleService( zkService,  schedulerFactoryBean,  eventHandler,  evenetListeners, rebalanceListeners);
    }
}
