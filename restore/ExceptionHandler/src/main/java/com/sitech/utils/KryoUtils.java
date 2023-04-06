package com.sitech.utils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.commons.codec.binary.Base64;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;

public class KryoUtils {

    public static <T extends Serializable> String objectCodeC(T object) {
        Kryo kryo = new Kryo();
        kryo.setReferences(false);
        kryo.register(object.getClass(), new JavaSerializer());
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Output output = new Output(baos);
        kryo.writeClassAndObject(output, object);
        output.flush();
        output.close();
        byte[] bytes = baos.toByteArray();
        try {
            baos.flush();
            baos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new String(new Base64().encode(bytes));
    }

    public static <T extends Serializable> T objectDeCodeC(String code, Class<T> clazz) {
        Kryo kryo = new Kryo();
        kryo.setReferences(false);
        kryo.register(clazz, new JavaSerializer());
        ByteArrayInputStream bais = new ByteArrayInputStream(new Base64().decode(code));
        Input input = new Input(bais);
        return (T) kryo.readClassAndObject(input);
    }
}
