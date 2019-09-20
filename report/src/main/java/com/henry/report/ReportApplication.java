package com.henry.report;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.SpringApplication;
/**
 * @Author: HongZhen
 * @Description:
 * @Date: Create in 2019/9/20 11:10
 **/

// 添加注解 @SpringBootApplication ，表示该类是一个启动类
@SpringBootApplication
public class ReportApplication {

    public static void main(String[] args) {
        SpringApplication.run(ReportApplication.class, args);
    }
}
