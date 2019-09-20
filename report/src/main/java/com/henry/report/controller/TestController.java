package com.henry.report.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Author: HongZhen
 * @Description:
 * @Date: Create in 2019/9/20 11:19
 **/

// 表示这是一个 Controller，并且其中所有的方法都是带有 @ResponseBody 的注解
@RestController
public class TestController{

//    为了能访问到该方法，需要添加如下注解，参数是代表如何来请求
    @RequestMapping("/test")
    public String test(String json){
        System.out.println(json);
        return json;
    }
}
