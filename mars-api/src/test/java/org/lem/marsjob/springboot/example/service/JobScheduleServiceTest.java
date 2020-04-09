package org.lem.marsjob.springboot.example.service;

import org.junit.Before;
import org.junit.Test;
import org.lem.marsjob.pojo.JobParam;
import org.lem.marsjob.service.JobScheduleService;
import org.lem.marsjob.zk.ZkService;
import org.quartz.SchedulerException;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;

public class JobScheduleServiceTest {
    JobScheduleService jobScheduleService;

    @Before
    public void init() throws Exception {
        SchedulerFactoryBean schedulerFactoryBean = new SchedulerFactoryBean();
        schedulerFactoryBean.afterPropertiesSet();
        ZkService zkService = new ZkService("localhost", "mars",7,7,schedulerFactoryBean);
        zkService.init();

        jobScheduleService = new JobScheduleService(zkService,null);
        jobScheduleService.afterPropertiesSet();
    }

    @Test
    public void testAddJob() throws Exception {
        JobParam jobParam = new JobParam(Job.class, "* * * * * ?", "mars", "mars");
        jobScheduleService.addJob(jobParam, false);
        Thread.sleep(10000);
    }

    @Test
    public void testDeleteJob() throws Exception {
        JobParam jobParam = new JobParam(Job.class, "* * * * * ?", "mars", "mars");
        jobScheduleService.deleteJob(jobParam,true);
        Thread.sleep(10000);
    }
}
