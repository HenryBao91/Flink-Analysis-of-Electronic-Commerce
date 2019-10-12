package com.henry.report.bean;

/**
 * @Author: Henry
 * @Description: 消息实体类
 * @Date: Create in 2019/10/11 23:40
 **/
public class Message {

    // 消息次数
    private int count;

    // 消息的时间戳
    private long timestamp;

    // 消息体
    private String message;

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        return "Message{" +
                "count=" + count +
                ", timestamp=" + timestamp +
                ", message='" + message + '\'' +
                '}';
    }
}
