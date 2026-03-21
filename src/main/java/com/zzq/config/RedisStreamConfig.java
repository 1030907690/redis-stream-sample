package com.zzq.config;


import com.zzq.listener.MyStreamConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;

import java.time.Duration;

/**
 * @description:
 * @author: Zhou Zhongqing
 * @date: 3/19/2026 10:28 PM
 */
@Configuration
public class RedisStreamConfig {

    private final Logger log = LoggerFactory.getLogger(RedisStreamConfig.class);
    @Autowired
    private MyStreamConsumer myStreamConsumer;

    public static final String STREAM_KEY = "my-stream";

    public static final String GROUP_NAME = "user-group";

    @Bean
    public StreamMessageListenerContainer<String, MapRecord<String, String, String>> streamMessageListenerContainer(StringRedisTemplate stringRedisTemplate) {
        //  容器选项配置
        // 泛型改为 MapRecord<String, String, String>
        StreamMessageListenerContainer.StreamMessageListenerContainerOptions<String, MapRecord<String, String, String>> options =
                StreamMessageListenerContainer.StreamMessageListenerContainerOptions
                        .builder()
                        .batchSize(10)
                        .pollTimeout(Duration.ofSeconds(2))
                        .build();

        //  初始化容器
        StreamMessageListenerContainer<String, MapRecord<String, String, String>> container =
                StreamMessageListenerContainer.create(stringRedisTemplate.getRequiredConnectionFactory(), options);

        // 消费组配置 (重点)
        // 自动创建消费组的健壮性处理
        createGroupSafely(stringRedisTemplate);

        //  注册消费者
        container.receive(
                Consumer.from(GROUP_NAME, "consumer-1"), // 消费组名和消费者实例名
                StreamOffset.create(STREAM_KEY, ReadOffset.lastConsumed()), // 从最后一次消费的位置开始
                myStreamConsumer
        );

        //  启动容器
        container.start();
        return container;
    }

    private void createGroupSafely(StringRedisTemplate stringRedisTemplate) {
        try {
            stringRedisTemplate.opsForStream().createGroup(STREAM_KEY, GROUP_NAME);
        } catch (Exception e) {
            // 生产环境下，如果组已存在会抛异常，这里直接捕获忽略即可
            log.info("消费组已存在或初始化跳过: {}", e.getMessage());
        }
    }
}