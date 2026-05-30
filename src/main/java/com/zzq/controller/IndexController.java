package com.zzq.controller;


import com.zzq.utils.RedisStreamUtil;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 *
 * @author: Zhou Zhongqing
 * @date: 1/18/2026 9:50 PM
 */
@Tag(name = "首页")
@RestController
@RequestMapping("/api/index")
public class IndexController {

    @Autowired
    private RedisStreamUtil redisStreamProducer;

    @GetMapping("/")
    @Operation(summary = "首页接口")
    public String index() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String formattedStr = LocalDateTime.now().format(formatter);
        redisStreamProducer.sendObjectWithLimit("测试数据"+ formattedStr);
        return "index";
    }

}
