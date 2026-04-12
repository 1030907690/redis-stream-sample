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
import org.springframework.util.Assert;

import java.time.LocalDateTime;
import java.time.ZoneId;
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
        sendObjectWithLimit(data, 10000, LocalDateTime.now());
    }

    public void sendObjectWithLimit(String data,LocalDateTime consumerDateTime) {
        sendObjectWithLimit(data, 10000, consumerDateTime);
    }

    /**
     * 发送数据到队列，带有限制长度
     * @param data  数据
     * @param maxLen  最大长度，近似修剪方式
     * @param consumerDateTime  真正消费的时间，为了做延迟队列
     */
    public void sendObjectWithLimit(String data, long maxLen, LocalDateTime consumerDateTime) {
        Assert.notNull(data, "data must not be null");
        Assert.notNull(consumerDateTime, "consumerDateTime must not be null");

        HashMap<String, String> map = new HashMap<>();
        map.put("data", data);
        map.put("time", String.valueOf(consumerDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()));
        MapRecord<String, String, String> record = StreamRecords.newRecord()
                .in(RedisStreamConfig.STREAM_KEY)
                .ofMap(map)
                .withId(RecordId.autoGenerate());

        RecordId recordId = stringRedisTemplate.opsForStream().add(record);
        log.info("已发送对象消息，ID: {}, 当前限流长度: {}", recordId, maxLen);

        //  限制长度（trim）  true 近似修剪
        stringRedisTemplate.opsForStream().trim(RedisStreamConfig.STREAM_KEY, maxLen, true);
    }
}
