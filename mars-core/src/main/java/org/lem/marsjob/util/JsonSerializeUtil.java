package org.lem.marsjob.util;

import com.alibaba.fastjson.JSON;

import java.nio.charset.Charset;

public final class JsonSerializeUtil {
    public static final Charset CHARSET_UTF8 = Charset.forName("UTF-8");

    private JsonSerializeUtil() {
    }

    public static byte[] encode(Object obj) {
        return toJson(obj).getBytes(CHARSET_UTF8);
    }

    public static String toJson(Object obj) {
        return JSON.toJSONString(obj, false);
    }

    public static <T> T decode(byte[] data, Class<T> classOfT) {
        String json = new String(data, CHARSET_UTF8);
        return fromJson(json, classOfT);
    }

    public static <T> T fromJson(String json, Class<T> classOfT) {
        return JSON.parseObject(json, classOfT);
    }
}