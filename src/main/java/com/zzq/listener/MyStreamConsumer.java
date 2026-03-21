package com.zzq.listener;


import com.zzq.config.RedisStreamConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.core.StringRedisTemplate;
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

    private static final Logger log = LoggerFactory.getLogger(MyStreamConsumer.class);
    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public void onMessage(MapRecord<String, String, String> record) {
        Map<String, String> map = record.getValue();
        log.info("收到消息: {}", map);
        // TODO 业务处理

        stringRedisTemplate.opsForStream().acknowledge(RedisStreamConfig.STREAM_KEY, RedisStreamConfig.GROUP_NAME, record.getId());
    }
}