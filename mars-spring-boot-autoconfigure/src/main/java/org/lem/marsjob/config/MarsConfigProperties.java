package org.lem.marsjob.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "mars")
public class MarsConfigProperties {
    private String zkAddress;
    private String projectGroup;
    private Integer jobEventExpireDay=7;
    private Integer jobExecuteLogExpireDay=7;

    public MarsConfigProperties() {
    }

    public String getZkAddress() {
        return zkAddress;
    }

    public void setZkAddress(String zkAddress) {
        this.zkAddress = zkAddress;
    }

    public String getProjectGroup() {
        return projectGroup;
    }

    public void setProjectGroup(String projectGroup) {
        this.projectGroup = projectGroup;
    }

    public Integer getJobEventExpireDay() {
        return jobEventExpireDay;
    }

    public void setJobEventExpireDay(Integer jobEventExpireDay) {
        this.jobEventExpireDay = jobEventExpireDay;
    }

    public Integer getJobExecuteLogExpireDay() {
        return jobExecuteLogExpireDay;
    }

    public void setJobExecuteLogExpireDay(Integer jobExecuteLogExpireDay) {
        this.jobExecuteLogExpireDay = jobExecuteLogExpireDay;
    }
}
