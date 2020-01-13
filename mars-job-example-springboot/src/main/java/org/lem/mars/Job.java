package org.lem.mars;

import org.lem.marsjob.annotation.SimpleJobSchedule;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
public class Job {
    @SimpleJobSchedule(cron = "* * * * * ?", group = "a", name = "b", startTime = "2019-01-21 12:00:00", endTime = "2020-01-21 18:00:00")
    public void say() {
        System.out.println("-------say--------------");
    }

    @SimpleJobSchedule(cron = "${testabcd}", group = "a", name = "asdad", startTime = "2019-01-21 12:00:00", endTime = "2019-05-21 18:00:00")
    private void say1024() {
        System.out.println("-------say1024--------------");
    }

    @PostConstruct
    private void say3(){
        System.out.println("----------------say3-------------");
    }


}
