import org.junit.Test;
import org.lem.marsjob.config.MarsJobAuoConfiguration;
import org.lem.marsjob.service.JobScheduleService;
import org.lem.marsjob.zk.ZkService;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

public class MarsJobAutoConfiguationTest {
    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(MarsJobAuoConfiguration.class));
    @Test
    public void serviceNameCanBeConfigured() {
        this.contextRunner.withPropertyValues("mars.zk_address=localhost:2181","mars.project_group=mars").run((context) -> {
            ZkService service=context.getBean(ZkService.class);
            JobScheduleService jobScheduleService=context.getBean(JobScheduleService.class);
            System.out.println(service);
        });
    }
}
