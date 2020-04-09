package org.lem.marsjob.pojo;

import org.quartz.JobExecutionContext;


public interface JobHandler {

    void execute(ExecuteJobParam executeJobParam);
}
