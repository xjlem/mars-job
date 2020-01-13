package org.lem.marsjob.serialize;

import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.lem.marsjob.util.KryoUtil;

public class ZkKryoSerialize implements ZkSerializer {
    @Override
    public byte[] serialize(Object data) throws ZkMarshallingError {
        return KryoUtil.writeClassAndObject(data);
    }

    @Override
    public Object deserialize(byte[] bytes) throws ZkMarshallingError {
        return KryoUtil.readClassAndObject(bytes);
    }
}
