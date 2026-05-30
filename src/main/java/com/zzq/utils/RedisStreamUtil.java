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
import java.util.Map;

/**
 *
 * @author: Zhou Zhongqing
 * @since: 3/19/2026 10:22 PM
 */
@Service
public class RedisStreamUtil {

    private final Logger log = LoggerFactory.getLogger(RedisStreamUtil.class);


    private static final String KEY_DATE = "data";

    @Autowired
    private StringRedisTemplate stringRedisTemplate;



    public void sendObjectWithLimit(String data) {
        sendObjectWithLimit(data, 10000);
    }

    /**
     * 发送数据到队列，带有限制长度
     * @param data  数据
     * @param maxLen  最大长度，近似修剪方式
     */
    public void sendObjectWithLimit(String data, long maxLen) {
        Assert.notNull(data, "data must not be null");

        HashMap<String, String> map = new HashMap<>();
        map.put(KEY_DATE, data);
        MapRecord<String, String, String> record = StreamRecords.newRecord()
                .in(RedisStreamConfig.STREAM_KEY)
                .ofMap(map)
                .withId(RecordId.autoGenerate());

        RecordId recordId = stringRedisTemplate.opsForStream().add(record);
        log.info("已发送对象消息，ID: {}, 当前限流长度: {}", recordId, maxLen);

        //  限制长度（trim）  true 近似修剪
        stringRedisTemplate.opsForStream().trim(RedisStreamConfig.STREAM_KEY, maxLen, true);
    }


    public Map<String, String> convert(Map<Object, Object> value) {
        Map<String, String> result = new HashMap<>();
        result.put(KEY_DATE, String.valueOf(value.get(KEY_DATE)));
        return result;
    }


}
