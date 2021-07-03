package org.hxl.realtime.annotation;

/**
 * @author Grant
 * @create 2021-07-03 18:58
 */
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.FIELD)  // 这个注解只能用在成员变量上
@Retention(RetentionPolicy.RUNTIME) // 定义注解的保留策略
public @interface NoSink {
}

