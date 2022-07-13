package flink.test;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class ByteDeSerializer implements DeserializationSchema<byte[]> {
    @Override
    public byte[] deserialize(byte[] bytes)  {
        return bytes;
    }

    @Override
    public boolean isEndOfStream(byte[] bytes) {
        return false;
    }

    @Override
    public TypeInformation<byte[]> getProducedType() {
        return TypeInformation.of(new TypeHint<byte[]>(){});
    }
}
