package com.henry.report.util;

import com.alibaba.fastjson.JSONObject;
import com.henry.report.bean.Clicklog;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * @Author: Henry
 * @Description: 点击流日志模拟器
 * @Date: Create in 2019/10/13 20:00
 **/
public class ClickLogGenerator {

    // ID 信息
    private static Long[] channelID = new Long[]{1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L, 19L, 20L};
    private static Long[] categoryID = new Long[]{1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L, 19L, 20L};
    private static Long[] produceID = new Long[]{1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L, 19L, 20L};
    private static Long[] userID = new Long[]{1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L, 19L, 20L};

    // 地区
    private static String[] contrys = new String[]{"china"};  // 地区-国家集合
    private static String[] provinces = new String[]{"HeNan", "HeBeijing"};  // 地区-省集合
    private static String[] citys = new String[]{"ShiJiaZhuang", "ZhengZhou", "LuoyYang"};  // 地区-市集合

    // 网络方式
    private static String[] networks = new String[]{"电信", "移动", "联通"};

    // 来源方式
    private static String[] sources = new String[]{"直接输入", "百度跳转", "360搜索跳转", "必应跳转"};

    // 浏览器
    private static String[] browser = new String[]{"火狐", "QQ浏览器", "360浏览器", "谷歌浏览器"};

    // 打开方式，离开时间
    private static List<Long[]> usertimeLog = producetimes();

    // 获取时间
    private static List<Long[]> producetimes() {
        List<Long[]> usertimelog = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            Long[] timearray = gettimes("2019-10-10 24:60:60:000");
            usertimelog.add(timearray);
        }
        return usertimelog;
    }

    private static Long[] gettimes(String time) {
        DateFormat dataFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss:SSS");
        try {
            Date date = dataFormat.parse(time);
            long timetemp = date.getTime();
            Random random = new Random();
            int randomint = random.nextInt(10);
            long starttime = timetemp - randomint*3600*1000;
            long endtime = starttime + randomint*3600*1000;
            return new  Long[]{starttime,endtime};
        }catch (ParseException e){
            e.printStackTrace();
        }
        return new Long[]{0L, 0L};
    }

    // 模拟发送 Http 请求到上报服务系统
    public static void send(String url, String json){
        try {
            CloseableHttpClient httpClient = HttpClientBuilder.create().build();
            HttpPost post = new HttpPost(url);
            JSONObject response = null ;
            try {
                StringEntity s = new StringEntity(json.toString(), "utf-8");
                s.setContentEncoding("utf-8");
                // 发送 json 数据需要设置 contentType
                s.setContentType("application/json");
                post.setEntity(s);

                HttpResponse res = httpClient.execute(post);
                if(res.getStatusLine().getStatusCode() == HttpStatus.SC_OK){
                    // 返回 json 格式
                    String result = EntityUtils.toString(res.getEntity());
                    System.out.println(result);
                }
            }catch (Exception e){
                throw new RuntimeException();
            }

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            // 频道id、类别id、产品id、用户id、打开时间、离开时间、地区、网络方式、来源方式、浏览器
            Clicklog clicklog = new Clicklog();

            clicklog.setChannelID(channelID[random.nextInt(channelID.length)]);
            clicklog.setCategoryID(categoryID[random.nextInt(channelID.length)]);
            clicklog.setProduceID(produceID[random.nextInt(produceID.length)]);
            clicklog.setUserID(userID[random.nextInt(userID.length)]);
            clicklog.setCountry(contrys[random.nextInt(contrys.length)]);
            clicklog.setProvince(provinces[random.nextInt(provinces.length)]);
            clicklog.setCity(citys[random.nextInt(citys.length)]);
            clicklog.setNetwork(networks[random.nextInt(networks.length)]);
            clicklog.setSource(sources[random.nextInt(sources.length)]);
            clicklog.setBrowserType(browser[random.nextInt(browser.length)]);

            Long[] times = usertimeLog.get(random.nextInt(usertimeLog.size()));
            clicklog.setEntryTime(times[0]);
            clicklog.setLeaveTime(times[1]);

            // 将点击流日志转成字符串，发送到前端地址
            String jsonstr = JSONObject.toJSONString(clicklog);
            System.out.println(jsonstr);
            try {
                Thread.sleep(100);
            }catch (InterruptedException e){
                e.printStackTrace();
            }

            send("http://localhost:1234/receive", jsonstr);
        }
    }
}
