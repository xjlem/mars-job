import org.junit.Test;
import org.junit.runner.RunWith;
import org.lem.marsjob.zk.ZkService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class MarsExampleSpringbootApplicationTests {

    @Autowired
    private ZkService zkService;

    @Test
    public void contextLoads() {
    }
    @Test
    public void testClear(){
        zkService.clearOldZkValue();
    }

}

