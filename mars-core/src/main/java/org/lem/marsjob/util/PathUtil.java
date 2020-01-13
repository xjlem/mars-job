package org.lem.marsjob.util;

public class PathUtil {
    public static final String MARS_JOB = "mars";

    public static String getPath(String... vars) {
        StringBuilder sb = new StringBuilder();
        for (String var : vars) {
            if (var.startsWith("/"))
                sb.append(var);
            else
                sb.append("/").append(var);
        }
        return sb.toString();
    }

}
