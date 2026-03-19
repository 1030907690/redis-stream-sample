package com.zzq.listener;


import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * @description:
 * @author: Zhou Zhongqing
 * @date: 3/19/2026 10:29 PM
 */
@Component
public class MyStreamConsumer implements StreamListener<String, MapRecord<String, String, String>> {
    @Override
    public void onMessage(MapRecord<String, String, String> record) {
        Map<String, String> map = record.getValue();

        // TODO 业务处理
    }
}