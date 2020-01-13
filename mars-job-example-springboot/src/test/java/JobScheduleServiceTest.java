import org.junit.Test;
import org.junit.runner.RunWith;
import org.lem.mars.SimpleJob;
import org.lem.marsjob.pojo.JobParam;
import org.lem.marsjob.service.JobScheduleService;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.impl.matchers.GroupMatcher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RunWith(SpringRunner.class)
@SpringBootTest
@ComponentScan(basePackages = {"org.lem.marsjob.*"})
public class JobScheduleServiceTest {
    @Autowired
    JobScheduleService jobScheduleService;


    @Test
    public void testAddJob() throws SchedulerException, InterruptedException {
        JobParam jobParam = new JobParam(SimpleJob.class, "* * * * * ?", "mars1", "mars1");
        jobScheduleService.addJob(jobParam, true);
        Thread.sleep(10000);
    }

    @Test
    public void testAddJobBench() throws SchedulerException, InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        for (int i = 0; i < 10; i++) {
            final int y = i;
            executorService.execute(() -> {
                for (int k = y * 10; k < (y + 1) * 10; k++) {
                    JobParam jobParam = new JobParam(SimpleJob.class, "* * * * * ?", k + "", k + "");
                    try {
                        jobScheduleService.addJob(jobParam, true);
                    } catch (SchedulerException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        Thread.sleep(1000000);

        say();
    }


    @Test
    public void testDeleteJobBench() throws SchedulerException, InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(100);
        for (int i = 0; i < 100; i++) {
            final int y = i;
            executorService.execute(() -> {
                for (int k = y * 10; k < (y + 1) * 10; k++) {
                    JobParam jobParam = new JobParam(SimpleJob.class, "* * * * * ?", k + "", k + "");
                    try {
                        jobScheduleService.deleteJob(jobParam, true);
                    } catch (SchedulerException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        Thread.sleep(10000);

        say();
    }

    private void say() throws SchedulerException {
        Set<String> allScheduledJobs = new HashSet<String>();
        Scheduler scheduler = jobScheduleService.getSchedulerFactoryBean().getScheduler();
        for (String groupName : scheduler.getJobGroupNames()) {

            for (JobKey jobKey : scheduler.getJobKeys(GroupMatcher.jobGroupEquals(groupName))) {

                String jobName = jobKey.getName();
                String jobGroup = jobKey.getGroup();

                //get job's trigger
                List<Trigger> triggers = (List<Trigger>) scheduler.getTriggersOfJob(jobKey);
                Date nextFireTime = triggers.get(0).getNextFireTime();

//                System.out.println("[jobName] : " + jobName + " [groupName] : "
//                        + jobGroup + " - " + nextFireTime);
                allScheduledJobs.add("[jobName] : " + jobName + " [groupName] : "
                        + jobGroup + " - " + nextFireTime);
            }
        }
        System.out.println(allScheduledJobs.size());
        System.out.println(allScheduledJobs);
    }

    @Test
    public void testDeleteJob() throws SchedulerException, InterruptedException {
        JobParam jobParam = new JobParam(SimpleJob.class, "* * * * * ?", "mars1", "mars1");
        jobScheduleService.deleteJob(jobParam, true);
        Thread.sleep(10000);
    }




}
