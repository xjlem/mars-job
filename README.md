#	分布式任务调度框架使用
## 概述
    该框架可以做什么
    1. 继承simplejob保证多机同时只有一台机器执行任务
    2. 继承shardingjob可以获取分片，多机协调完成任务
    3. 任务在一台机器上添加，可以选择同步到集群上，方便集群间任务启停同步
## 快速开始
### 1.POM依赖
 ```$xslt
<dependency>
    <groupId>org.lem</groupId>
    <artifactId>mars-api</artifactId>        
    <version>0.0.1-SNAPSHOT</version>
 </dependency>
 ```
### 2.启用任务调度 @EnableMarsScheduling
### 3.所需配置，框架自带quartz调度如需修改配置添加quartz.properties到项目中
 ```$xslt
zk:
  address: localhost:2181  #注册的zk
project:
  group: mars     #项目名
job_event:
    expire_day: 7  #事务日志（删除七天前的事务日志）
job_execute_log:
    expire_day: 7  #任务执行日志清理时间（删除七天前的日志）
```
### 4.添加任务 使用spring自动注入JobScheduleService jobScheduleService
```$xslt
 @Autowired
 private JobScheduleService jobScheduleService;
 ------------------------------
 JobParam jobParam=new JobParam(SimpleJob.class,"0 * * * * ?","0","0");
 jobScheduleService.addJob(jobParam,false);
```
addjob方法第一个参数为任务配置，第二个为是否同步到集群上，同理还有updateJob，deleteJob等方法
<br/>jobParam中的jobData可以在任务执行中被获取
```$xslt
 Map<String, String> jobMap = new HashMap<>();
 jobMap.put("property1", "1234");
 JobParam jobParam = new JobParam(DelegateSimpleJob.class, scheduled.cron(), group, name);
 jobParam.setJobData(jobMap);
```    
```$xslt
public class ExampleJob extends SimpleJobExecutor {
    @Override
    protected void internalJobExecute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        try {
            String property1 = (String) jobExecutionContext.getMergedJobDataMap().get("property1");
        } catch (Exception e) {
            throw new RuntimeException("execute job fail",e);
        }
    }
}
```
### 5.spring 注解
```$xslt
@Component
public class Job {
    @SimpleJobSchedule(cron = "* * * * * ?", group = "a", name = "b", startTime = "2019-01-21 12:00:00", endTime = "2019-01-21 18:00:00")
    public void say() {
        System.out.println("-------say--------------");
    }
}
```
其中cron为必填项，group默认为类名，name默认为方法名，同一方法可以有多个注解
### 6. 例子
    如不使用spring框架，可参考mars-job-example中mars-example-java模块
    