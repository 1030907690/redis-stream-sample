package com.zzq.controller;


import com.zzq.utils.RedisStreamUtil;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;

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
        redisStreamProducer.sendObjectWithLimit("张三"+ LocalDateTime.now());
        return "index";
    }

}
