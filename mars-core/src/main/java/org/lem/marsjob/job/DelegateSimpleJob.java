package org.lem.marsjob.job;

import org.lem.marsjob.pojo.ExecuteJobParam;
import org.springframework.context.ApplicationContext;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;

public class DelegateSimpleJob extends SimpleJobExecutor {

    @Override
    public void execute(ExecuteJobParam executeJobParam) {
        try {
            ApplicationContext applicationContext = (ApplicationContext) executeJobParam.getContext();
            String clazzName = executeJobParam.getJobDataMap().get("class");
            String methodName = executeJobParam.getJobDataMap().get("method");
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
