package org.lem.marsjob.pojo;


import org.lem.marsjob.enums.Operation;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;

public class JobParam<T> implements Serializable {
    private Date startTime;
    private Date endTime;
    private Class<T> jobClass;
    private String cron;
    private Map<String,String> jobData;
    private String jobGroup;
    private String jobName;
    private Integer operationCode = -1;
    //是否是竞争模式
    private boolean balance=false;

    public JobParam(Class<T> jobClass, String cron, String jobGroup, String jobName) {
        this.jobClass = jobClass;
        this.cron = cron;
        this.jobGroup = jobGroup;
        this.jobName = jobName;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    public Class<T> getJobClass() {
        return jobClass;
    }

    public void setJobClass(Class<T> jobClass) {
        this.jobClass = jobClass;
    }

    public String getCron() {
        return cron;
    }

    public void setCron(String cron) {
        this.cron = cron;
    }


    public Map<String, String> getJobData() {
        return jobData;
    }

    public void setJobData(Map<String, String> jobData) {
        this.jobData = jobData;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getJobGroup() {
        return jobGroup;
    }

    public void setJobGroup(String jobGroup) {
        this.jobGroup = jobGroup;
    }

    public Integer getOperationCode() {
        return operationCode;
    }

    public void setOperationCode(Integer operationCode) {
        this.operationCode = operationCode;
    }

    public boolean isBalance() {
        return balance;
    }

    public void setBalance(boolean balance) {
        this.balance = balance;
    }

    @Override
    public String toString() {
        return "JobParam{" +
                "startTime=" + startTime +
                ", endTime=" + endTime +
                ", jobClass=" + jobClass +
                ", cron='" + cron + '\'' +
                ", jobData=" + jobData +
                ", jobName='" + jobName + '\'' +
                ", jobGroup='" + jobGroup + '\'' +
                ", operation=" + Operation.getOperationByCode(operationCode) +
                '}';
    }
}
