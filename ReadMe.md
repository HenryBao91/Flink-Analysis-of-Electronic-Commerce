### 4、工程结构
![screenshot](screenshot/1.png)

### 5、上报服务系统
![screenshot](screenshot/2.png)
#### 5.1、Spring Boot
上报服务系统是一个 Java Web 工程，为了快速开发 web 项目，采用 JavaWeb 最流行的 Spring Boot。

Spring Boot 是一个基于 Spring 之上的快速应用快速构建框架。Spring Boot 主要解决两方面问题：

- 依赖太多问题
    - 轻量级 JavaEE 开发，需要导入大量的依赖
    - 依赖直接还存在版本冲突问题
- 配置太多问题
    - 大量的 XML 配置
    
**开发 Spring Boot 程序的基本步骤:**
   - 导入 Spring Boot 依赖（起步依赖）
   - 编写 `application.properties` 配置文件
   - 编写 `Application` 入口程序
   
#### 5.2、配置 Maven 本地仓库
#### 5.3、导入 Maven 依赖
[pom文件](/report/pom.xml)
[配置文件](/resources/application.properties)

#### 5.4、创建项目包结构

包名 | 说明
---|---
`com.henry.report.controller` | 存放 Spring MVC 的controller 
`com.henry.report.bean` | 存放相关的 Java Bean 实体类
`com.henry.report.util` | 存放相关的工具类

#### 5.5、验证 Spring Boot 工程是否创建成功
**步骤:**
1. 创建 SpringBoot 入口程序 Application
2. 创建`application.properties` 配置文件
3. 编写一个简单的`Spring MVC` Controller/Handler，接收浏览器请求参数并打印回显
4. 打开浏览器测试

**实现:**
1. 创建 SpringBoot 入口程序`ReportApplication`，用来启动 SpringBoot 程序
    - 在类上添加注解
    ```
    @SpringBootApplication
    ```
    - 在 main 方法中添加代码，用来运行 Spring Boot 程序
    ```
    SpringApplication.run(ReportApplication.class);
    ```
2. 创建一个`TestController`
    在该类上要添加注解
    ```java
    @RestController
    public class TestController{
    
    }
    ```
3. 编写一个`test Handler`
    从浏览器上接收一个名为 json 的参数，并打印显示
    ```java
    @RequestMapping("/test")
    public String test(String json){
        System.out.println(json);
        return json;
    }
    ```
 
4. 编写配置文件
    - 配置端口号
    ```properties
       server.port=8888
    ```

5. 启动 Spring Boot 程序
6. 打开浏览器测试 Handler 是否能够接收到数据
[访问连接]()