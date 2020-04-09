package org.lem.marsjob.pojo;

import java.util.Date;
import java.util.Map;

public class ExecuteJobParam {
    private Class<? extends JobHandler> jobHandler;
    private Map<String, String> jobDataMap;
    private String  jobIdentity;
    private Date fireTime;
    private Object context=null;

    public ExecuteJobParam(JobParam jobParam,Date fireTime) {
        this.fireTime = fireTime;
        jobHandler=jobParam.getJobClass();
        jobDataMap=jobParam.getJobData();
        jobIdentity=jobParam.getJobGroup()+"_"+jobParam.getJobName();
    }

    public Class<? extends JobHandler> getJobHandler() {
        return jobHandler;
    }

    public void setJobHandler(Class<? extends JobHandler> jobHandler) {
        this.jobHandler = jobHandler;
    }

    public Map<String, String> getJobDataMap() {
        return jobDataMap;
    }

    public void setJobDataMap(Map<String, String> jobDataMap) {
        this.jobDataMap = jobDataMap;
    }

    public String getJobIdentity() {
        return jobIdentity;
    }

    public void setJobIdentity(String jobIdentity) {
        this.jobIdentity = jobIdentity;
    }

    public Date getFireTime() {
        return fireTime;
    }

    public void setFireTime(Date fireTime) {
        this.fireTime = fireTime;
    }

    public Object getContext() {
        return context;
    }

    public void setContext(Object context) {
        this.context = context;
    }
}
