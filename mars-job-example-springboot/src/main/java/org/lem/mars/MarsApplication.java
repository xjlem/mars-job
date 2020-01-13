package org.lem.mars;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = {"org.lem.mars"})
public class MarsApplication {

    public static void main(String[] args) {
        ConfigurableApplicationContext context=SpringApplication.run(MarsApplication.class, args);
        System.out.println(context.getEnvironment());
    }


}

