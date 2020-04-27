package org.lem.mars;

import jdk.nashorn.internal.runtime.linker.Bootstrap;
import org.lem.marsjob.annotation.SimpleJobSchedule;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

@Component
public class Job {
    @SimpleJobSchedule(cron = "* * * * * ?", group = "a", name = "2", startTime = "2019-01-21 12:00:00", endTime = "2020-05-21 18:00:00",balance = true)
    public void say() {
        System.out.println("-------say--------------");
    }

    @SimpleJobSchedule(cron = "* * * * * ?", group = "a", name = "1", startTime = "2019-01-21 12:00:00", endTime = "2020-05-21 18:00:00",balance = true)
    private void say1024() {
        System.out.println("-------say1024--------------");
    }

    @PostConstruct
    private void say3(){
        System.out.println("----------------say3-------------");
    }

    public static void main(String[] args) {
    }
}
