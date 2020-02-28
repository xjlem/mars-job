package org.lem.marsjob.annotation;


import com.google.common.base.Strings;
import org.lem.marsjob.job.DelegateSimpleJob;
import org.lem.marsjob.pojo.JobParam;
import org.lem.marsjob.service.JobScheduleService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.EmbeddedValueResolverAware;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.util.Assert;
import org.springframework.util.StringValueResolver;

import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


public class MarsScheduledAnnotationBeanPostProcessor
        implements BeanPostProcessor, EmbeddedValueResolverAware {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private StringValueResolver resolver;
    private static ThreadLocal<SimpleDateFormat> MS_SDF = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
    };


    @Autowired
    private JobScheduleService jobScheduleService;



    public MarsScheduledAnnotationBeanPostProcessor() {
    }





    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) {
        return bean;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) {

        Class<?> targetClass = AopProxyUtils.ultimateTargetClass(bean);
        Map<Method, Set<SimpleJobSchedule>> annotatedMethods = MethodIntrospector.selectMethods(targetClass,
                (MethodIntrospector.MetadataLookup<Set<SimpleJobSchedule>>) method -> {
                 /*   Set<SimpleJobSchedule> scheduledMethods = new HashSet<>();
                    SimpleJobSchedule simpleJobSchedule = method.getAnnotation(SimpleJobSchedule.class);
                    if (simpleJobSchedule != null)
                        scheduledMethods.add(simpleJobSchedule);*/
                    Set<SimpleJobSchedule> scheduledMethods = AnnotatedElementUtils.getMergedRepeatableAnnotations(
                            method, SimpleJobSchedule.class, SimpleJobSchedules.class);
                    return (!scheduledMethods.isEmpty() ? scheduledMethods : null);
                });
        if (annotatedMethods.isEmpty()) {
            if (logger.isTraceEnabled()) {
                logger.trace("No @SimpleJobSchedule annotations found on bean class: " + targetClass);
            }
        } else {
            // Non-empty set of methodsl
            annotatedMethods.forEach((method, scheduledMethods) ->
                    scheduledMethods.forEach(scheduled -> processScheduled(scheduled, method, bean)));
            if (logger.isTraceEnabled()) {
                logger.trace(annotatedMethods.size() + " @SimpleJobSchedule methods processed on bean '" + beanName +
                        "': " + annotatedMethods);
            }
        }
        return bean;
    }

    protected void processScheduled(SimpleJobSchedule scheduled, Method method, Object bean) {
        Assert.isTrue(method.getParameterCount() == 0, "Only no-arg methods may be annotated with @Scheduled");
        String group = scheduled.group();
        String name = scheduled.name();
        String cron = resolver.resolveStringValue(scheduled.cron());
        Map<String, String> jobMap = new HashMap<>();
        jobMap.put("class", bean.getClass().getName());
        jobMap.put("method", method.getName());
        if (Strings.isNullOrEmpty(group))
            group = bean.getClass().getName();
        if (Strings.isNullOrEmpty(name))
            name = method.getName();
        try {
            JobParam jobParam = new JobParam(DelegateSimpleJob.class, cron, group, name);
            if (!Strings.isNullOrEmpty(scheduled.startTime()))
                jobParam.setStartTime(MS_SDF.get().parse(scheduled.startTime()));
            if (!Strings.isNullOrEmpty(scheduled.endTime()))
                jobParam.setEndTime(MS_SDF.get().parse(scheduled.endTime()));
            jobParam.setJobData(jobMap);
            jobParam.setBalance(scheduled.balance());
            jobScheduleService.addJob(jobParam, false);
            logger.info("添加注解定时任务，class:{},method:{},cron:{},group:{},name:{}", jobMap.get("class"), jobMap.get("method"), cron, group, name);
        } catch (Exception e) {
            logger.error("初始化job失败", e);
            throw new RuntimeException(e);
        }
    }




    @Override
    public void setEmbeddedValueResolver(StringValueResolver resolver) {
        this.resolver = resolver;
    }
}
