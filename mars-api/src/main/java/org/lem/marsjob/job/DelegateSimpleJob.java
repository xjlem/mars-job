package org.lem.marsjob.job;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.context.ApplicationContext;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;

public class DelegateSimpleJob extends SimpleJobExecutor {

    @Override
    protected void internalJobExecute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        try {
            ApplicationContext applicationContext = ((ApplicationContext) jobExecutionContext.getScheduler().getContext().get("applicationContext"));
            String clazzName = (String) jobExecutionContext.getMergedJobDataMap().get("class");
            String methodName = (String) jobExecutionContext.getMergedJobDataMap().get("method");
            Class clazz = Thread.currentThread().getContextClassLoader().loadClass(clazzName);
            Object delegateObject = applicationContext.getBean(clazz);
            Method method = clazz.getDeclaredMethod(methodName);
            ReflectionUtils.makeAccessible(method);
            method.invoke(delegateObject);
        } catch (Exception e) {
            throw new RuntimeException("execute job fail",e);
        }

    }
}
