import org.I0Itec.zkclient.serialize.SerializableSerializer;
import org.lem.marsjob.pojo.JobParam;
import org.lem.marsjob.util.KryoUtil;
import org.quartz.Job;

public class TestJobSyn {
    public void testJobSyn() {

    }

    public void testSerlize() {
        JobParam jobParam = new JobParam(Job.class, "* * * * * ?", "mars", "mars");
        byte[] kryoByte = KryoUtil.writeClassAndObject(jobParam);
        System.out.println(kryoByte.length);
        SerializableSerializer serializableSerializer = new SerializableSerializer();
        byte[] zkSerialize = serializableSerializer.serialize(jobParam);
        System.out.println(zkSerialize.length);
    }

    public static void main(String[] args) {
        JobParam jobParam = new JobParam(Job.class, "* * * * * ?", "mars", "mars");
        byte[] kryoByte = KryoUtil.writeClassAndObject(jobParam);
        JobParam jobParam2 =KryoUtil.readClassAndObject(kryoByte);
        System.out.println(kryoByte.length);
        SerializableSerializer serializableSerializer = new SerializableSerializer();
        byte[] zkSerialize = serializableSerializer.serialize(jobParam);
        System.out.println(zkSerialize.length);
    }
}
