package org.lem.marsjob.config;

import org.lem.marsjob.annotation.MarsScheduledAnnotationBeanPostProcessor;
import org.lem.marsjob.service.JobScheduleService;
import org.lem.marsjob.service.JobScheduleServiceFactory;
import org.lem.marsjob.zk.ZkService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
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



    @Bean(initMethod = "init", destroyMethod = "close")
    public ZkService zkService() {
        ZkService zkService = new ZkService(properties.getZkAddress(), properties.getProjectGroup(), properties.getJobEventExpireDay(), properties.getJobEventExpireDay());
        return zkService;
    }


    @Bean
    @ConditionalOnMissingBean
    public JobScheduleService createJobService(ZkService zkService, SchedulerFactoryBean schedulerFactoryBean) {
        JobScheduleServiceFactory factory=new JobScheduleServiceFactory(zkService,schedulerFactoryBean);
        return factory.build();
    }

    @Bean
    @ConditionalOnMissingBean
    public MarsScheduledAnnotationBeanPostProcessor createMarsScheduledAnnotationBeanPostProcessor() {
        MarsScheduledAnnotationBeanPostProcessor processor = new MarsScheduledAnnotationBeanPostProcessor();
        return processor;
    }



}
