package org.lem.marsjob.util;

import java.lang.reflect.Method;

public class ReflactUtil {

    public static Method getMethod(String methodName, Class clazz) {
        try {
            return clazz.getMethod(methodName);
        } catch (NoSuchMethodException e) {
        }
        return null;
    }
}
