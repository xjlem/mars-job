package org.lem.mars;

import org.lem.marsjob.annotation.SimpleJobSchedule;
import org.lem.marsjob.pojo.JobParam;
import org.lem.marsjob.service.JobScheduleService;
import org.quartz.SchedulerException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Set;

@Service
public class JobService implements InitializingBean {
    @Autowired
    private JobScheduleService jobScheduleService;

    private static ThreadLocal<SimpleDateFormat> MS_SDF = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
    };

    /*@Autowired
    private Job jobl;*/

    public void addShardingJobBySchedule() throws SchedulerException {
       /* JobParam jobParam=new JobParam();
        jobParam.setCron("* * * * * ?");
        JobDetail jobDetail = JobBuilder.newJob(ShardingJob.class).withIdentity("2").build();
        //jobParam.setJobDetail(jobDetail);
        jobScheduleService.addJob(jobParam,true);*/
    }

    public void addSimpleJob() throws SchedulerException, ParseException {
        JobParam jobParam = new JobParam(SimpleJob.class, "* * * * * ?", "0", "0");
        jobParam.setStartTime(MS_SDF.get().parse("2018-12-23 0:00:00"));
        jobParam.setEndTime(MS_SDF.get().parse("2020-04-23 0:00:00"));
        jobScheduleService.addJob(jobParam, false);
        JobParam jobParam2 = new JobParam(null, "0 0 0 * * ?", "1111", "1111111111");

        jobScheduleService.deleteJob(jobParam2,false);
    }


    @Override
    public void afterPropertiesSet() throws Exception {
        /*addSimpleJob();
        addShardingJobBySchedule();*/
        // addShardingJob();
    }


    @SimpleJobSchedule(cron = "* * * * * ?")
    private void say() throws SchedulerException {
        Set<String> allScheduledJobs = jobScheduleService.getScheduleJobsDetail();
        System.out.println(allScheduledJobs.size());
        System.out.println(allScheduledJobs);
    }


}
