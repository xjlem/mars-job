package org.lem.marsjob.config;

import org.lem.marsjob.annotation.MarsScheduledAnnotationBeanPostProcessor;
import org.lem.marsjob.mq.MqService;
import org.lem.marsjob.service.JobScheduleService;
import org.lem.marsjob.service.JobScheduleServiceFactory;
import org.lem.marsjob.service.JobSynService;
import org.lem.marsjob.zk.ZkService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.*;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;

import java.util.Properties;

@Configuration
@ConditionalOnClass(JobScheduleService.class)
@EnableConfigurationProperties(MarsConfigProperties.class)
public class MarsJobAuoConfiguration {

    protected Logger LOG = LoggerFactory.getLogger(this.getClass());

    public MarsJobAuoConfiguration(){
    }

    @Autowired
    private MarsConfigProperties properties;

    @Bean
    @ConditionalOnMissingBean
    public SchedulerFactoryBean schedulerFactoryBean() {
        SchedulerFactoryBean schedulerFactoryBean = new SchedulerFactoryBean();
        schedulerFactoryBean.setApplicationContextSchedulerContextKey("applicationContext");
        try {
            if (MarsJobAuoConfiguration.class.getClassLoader().getResource("quartz.properties") != null) {
                Properties quartzProperties = new Properties();
                quartzProperties.load(MarsJobAuoConfiguration.class.getClassLoader().getResourceAsStream("quartz.properties"));
                schedulerFactoryBean.setQuartzProperties(quartzProperties);
            }
        } catch (Exception e) {
            LOG.warn("can not load quartz properties");
        }
        return schedulerFactoryBean;
    }


    @ConditionalOnProperty(value = "mars.nameserver")
    @Bean(initMethod = "init", destroyMethod = "close")
    public MqService mqService(SchedulerFactoryBean schedulerFactoryBean) {
        MqService mqService = new MqService(properties.getZkAddress(),properties.getNameserver(), properties.getProjectGroup(), schedulerFactoryBean);
        if(properties.getJobMaxExecuteTime()!=null)
            mqService.setJobMaxExecuteTime(properties.getJobMaxExecuteTime());
        return mqService;
    }


    @ConditionalOnMissingBean(MqService.class)
    @Bean(initMethod = "init", destroyMethod = "close")
    public ZkService zkService(SchedulerFactoryBean schedulerFactoryBean) {
        ZkService zkService = new ZkService(properties.getZkAddress(), properties.getProjectGroup(), properties.getJobEventExpireDay(), properties.getJobEventExpireDay(),schedulerFactoryBean);
        return zkService;
    }




    @Bean
    @ConditionalOnMissingBean
    public JobScheduleService createJobService(JobSynService jobSynService) {
        JobScheduleServiceFactory factory=new JobScheduleServiceFactory(jobSynService);
        return factory.build();
    }

    @Bean
    @ConditionalOnMissingBean
    public MarsScheduledAnnotationBeanPostProcessor createMarsScheduledAnnotationBeanPostProcessor() {
        MarsScheduledAnnotationBeanPostProcessor processor = new MarsScheduledAnnotationBeanPostProcessor();
        return processor;
    }



}
