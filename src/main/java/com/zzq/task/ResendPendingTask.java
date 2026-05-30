package com.zzq.task;

import com.zzq.config.RedisStreamConfig;
import com.zzq.utils.MachineInfoUtil;
import com.zzq.utils.RedisStreamUtil;
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
import org.springframework.util.CollectionUtils;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

/**
 * @author zzq
 * @since 2026/03/21 14:35:14
 */
@Component
public class ResendPendingTask {

    private static final Logger log = LoggerFactory.getLogger(ResendPendingTask.class);

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private RedisStreamUtil redisStreamUtil;

    @Scheduled(fixedRate = 10000)
    public void resendPendingMessages() {

        PendingMessagesSummary summary = stringRedisTemplate.opsForStream().pending(RedisStreamConfig.STREAM_KEY, RedisStreamConfig.GROUP_NAME);

        if (summary != null && summary.getTotalPendingMessages() > 0) {
            // 读取前 10 条 Pending 消息
            // 由于有shouldConsumer逻辑存在，此处可能一直拉到相同10条数据
            PendingMessages pendingMessages = stringRedisTemplate.opsForStream()
                    .pending(RedisStreamConfig.STREAM_KEY, RedisStreamConfig.GROUP_NAME, Range.unbounded(), 10);

            pendingMessages.forEach(message -> {
                // 获取消息 ID 和已投递次数
                RecordId id = message.getId();
                Duration elapsed = message.getElapsedTimeSinceLastDelivery();

                // 3. 如果消息超过 30 秒还没处理完，说明原消费者可能挂了，重新处理
                if (elapsed.getSeconds() > 30) {
                    // 这里可以重新读取消息内容并执行业务，或者使用 XCLAIM 转移给其他消费者

                    // 核心优化：使用 claim 转移拥有权并直接获取消息内容
                    // 这相当于 Redis 的 XCLAIM 命令，它会重置该消息的 idle 时间，防止其他节点并发争抢
                    List<MapRecord<String, Object, Object>> claimedRecords = stringRedisTemplate.opsForStream().claim(
                            RedisStreamConfig.STREAM_KEY,
                            RedisStreamConfig.GROUP_NAME,
                            MachineInfoUtil.getHostName(), // 变成当前兜底消费者的名字
                            Duration.ofSeconds(30), // 期望的 idle 时间
                            id
                    );

                    if (CollectionUtils.isEmpty(claimedRecords)) {
                        return; // 说明可能被其他并发线程抢先处理了
                    }

                    for (MapRecord<String, Object, Object> record : claimedRecords) {
                        try {
                            Map<Object, Object> value = record.getValue();

                            if (redisStreamUtil.shouldConsumer(redisStreamUtil.convert(value))) {
                                log.info("重新处理消息: {}", value);

                                // TODO: 你的实际业务处理逻辑

                                // 处理成功，确认消息
                                stringRedisTemplate.opsForStream().acknowledge(
                                        RedisStreamConfig.STREAM_KEY,
                                        RedisStreamConfig.GROUP_NAME,
                                        record.getId()
                                );
                            } else {
                                // 如果暂不满足消费条件，由于上面 claim 时设置了 30s 的 idle，
                                // 它在接下来的 30 秒内不会再被重复捞起，腾出了空间给其他 pending 消息
                                log.debug("当前时间未达到消息设定的消费时间，暂不处理, ID: {}", record.getId());
                            }
                        } catch (Exception e) {
                            log.error("处理单条 Pending 消息失败, ID: " + record.getId(), e);
                            // 这里可以做重试次数累加，如果超过 3~5 次一直失败，建议人工介入或进入死信，防止死循环
                        }
                    }




                }
            });
        }
    }





}
