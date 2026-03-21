package com.zzq.utils;


import com.zzq.config.RedisStreamConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.HashMap;

/**
 * @description:
 * @author: Zhou Zhongqing
 * @date: 3/19/2026 10:22 PM
 */
@Service
public class RedisStreamProducer {

    private final Logger log = LoggerFactory.getLogger(RedisStreamProducer.class);

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    public void sendObjectWithLimit(String data) {
        sendObjectWithLimit(data, 10000);
    }

    public void sendObjectWithLimit(String data, long maxLen) {
        HashMap<String, String> map = new HashMap<>();
        map.put("data", data);
        MapRecord<String, String, String> record = StreamRecords.newRecord()
                .in(RedisStreamConfig.STREAM_KEY)
                .ofMap(map)
                .withId(RecordId.autoGenerate());

        RecordId recordId = stringRedisTemplate.opsForStream().add(record);

        //  限制长度（trim）  true 近似修剪
        stringRedisTemplate.opsForStream().trim(RedisStreamConfig.STREAM_KEY, maxLen, true);
        log.info("已发送对象消息，ID: {}, 当前限流长度: {}", recordId, maxLen);
    }
}
