package org.lem.marsjob.annotation;

import java.lang.annotation.*;

@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Repeatable(SimpleJobSchedules.class)
public @interface SimpleJobSchedule {
    /**
     * cron表达式.
     *
     * @return cron
     */
    String cron() default "";

    /**
     * 默认是类全名
     *
     * @return
     */
    String group() default "";

    /**
     * 默认是方法名
     *
     * @return
     */
    String name() default "";

    /**
     * yyyy-MM-dd HH:mm:ss
     *
     * @return
     */
    String startTime() default "";

    /**
     * yyyy-MM-dd HH:mm:ss
     */
    String endTime() default "";

    boolean balance() default false;

}
