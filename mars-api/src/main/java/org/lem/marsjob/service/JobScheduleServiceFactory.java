package org.lem.marsjob.service;

import com.google.common.base.Preconditions;
import org.lem.marsjob.EventHandler;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;

import java.util.ArrayList;
import java.util.List;

public class JobScheduleServiceFactory {
    private List<JobEvenetListener> evenetListeners=new ArrayList<>();

    private JobSynService jobSynService;


    public JobScheduleServiceFactory(JobSynService zkService) {
        this.jobSynService = zkService;
    }

    public List<JobEvenetListener> getEvenetListeners() {
        return evenetListeners;
    }

    public void setEvenetListeners(List<JobEvenetListener> evenetListeners) {
        this.evenetListeners = evenetListeners;
    }

    public JobScheduleService build(){
        Preconditions.checkNotNull(jobSynService);
        return new  JobScheduleService(jobSynService,  evenetListeners);
    }
}
