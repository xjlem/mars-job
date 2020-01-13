package org.lem.marsjob.annotation;

import java.lang.annotation.*;

@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface SimpleJobSchedules {

    SimpleJobSchedule[] value();

}
