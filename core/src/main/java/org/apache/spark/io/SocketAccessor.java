package org.apache.spark.io;

import sun.net.NetProperties;

import java.lang.reflect.Field;
import java.net.Socket;
import java.security.AccessController;
import java.security.PrivilegedAction;

public class SocketAccessor {
    private static final Field implField;

    static {
        try {
            implField = Socket.class.getDeclaredField("impl");
            implField.setAccessible(true);  // 反射突破访问控制
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    public static Object getImpl(Socket s) {
        try {
            return implField.get(s);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static Boolean usePlainSocketImpl() {
        PrivilegedAction<String> pa = () -> NetProperties.get("jdk.net.usePlainSocketImpl");
        @SuppressWarnings("removal")
        String str = AccessController.doPrivileged(pa);
        return (str != null) && !str.equalsIgnoreCase("false");
    }
}
