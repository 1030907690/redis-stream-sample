package com.zzq.task;

import com.zzq.config.RedisStreamConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.PendingMessagesSummary;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;

/**
 * @author zzq
 * @since 2026/03/21 14:35:14
 */
@Component
public class ResendPendingTask {

    private static final Logger log = LoggerFactory.getLogger(ResendPendingTask.class);

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Scheduled(fixedRate = 10000)
    public void resendPendingMessages() {

        PendingMessagesSummary summary = stringRedisTemplate.opsForStream().pending(RedisStreamConfig.STREAM_KEY, RedisStreamConfig.GROUP_NAME);

        if (summary != null) {
              // 读取前 10 条 Pending 消息
            PendingMessages pendingMessages = stringRedisTemplate.opsForStream()
                    .pending(RedisStreamConfig.STREAM_KEY, RedisStreamConfig.GROUP_NAME, Range.unbounded(), 10);

            pendingMessages.forEach(message -> {
                // 获取消息 ID 和已投递次数
                RecordId id = message.getId();
                Duration elapsed = message.getElapsedTimeSinceLastDelivery();

                // 3. 如果消息超过 30 秒还没处理完，说明原消费者可能挂了，重新处理
                if (elapsed.getSeconds() > 30) {
                    // 这里可以重新读取消息内容并执行业务，或者使用 XCLAIM 转移给其他消费者
                    List<MapRecord<String, Object, Object>> records = stringRedisTemplate.opsForStream().claim(
                            RedisStreamConfig.STREAM_KEY,
                            RedisStreamConfig.GROUP_NAME,
                            "consumer-1",
                            Duration.ofSeconds(1), // 只要空闲超过 1s 就认领
                            id
                    );

                    records.forEach(record -> {
                        log.info("重新处理消息: {}", record.getValue());

                        stringRedisTemplate.opsForStream().acknowledge(RedisStreamConfig.STREAM_KEY,  RedisStreamConfig.GROUP_NAME, record.getId());
                    });

                }
            });
        }
    }

}
