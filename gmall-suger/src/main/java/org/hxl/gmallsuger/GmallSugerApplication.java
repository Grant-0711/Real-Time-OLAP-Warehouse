package org.hxl.gmallsuger;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "org.hxl.gmallsuger.mapper")
public class GmallSugerApplication {

    public static void main(String[] args) {
        SpringApplication.run(GmallSugerApplication.class, args);
    }

}
