package org.lem.mars;

import org.lem.marsjob.annotation.SimpleJobSchedule;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
public class Job {
    @SimpleJobSchedule(cron = "0 * * * * ?", group = "a", name = "2", startTime = "2019-01-21 12:00:00", endTime = "2020-05-21 18:00:00",balance = true)
    public void say() {
        System.out.println("-------say--------------");
    }

    @SimpleJobSchedule(cron = "0 * * * * ?", group = "a", name = "1", startTime = "2019-01-21 12:00:00", endTime = "2020-05-21 18:00:00",balance = true)
    private void say1024() {
        System.out.println("-------say1024--------------");
    }

    @PostConstruct
    private void say3(){
        System.out.println("----------------say3-------------");
    }


}
