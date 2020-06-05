package com.wxmimperio.flink.bean;

public class Message {
    private String head;
    private String body;
    private String end;

    public Message() {
    }

    public Message(String head, String body, String end) {
        this.head = head;
        this.body = body;
        this.end = end;
    }

    public String getHead() {
        return head;
    }

    public void setHead(String head) {
        this.head = head;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    public String getEnd() {
        return end;
    }

    public void setEnd(String end) {
        this.end = end;
    }
}
