package com.zzq.controller;


import com.zzq.utils.RedisStreamProducer;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.*;
import java.time.LocalDateTime;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
    private RedisStreamProducer redisStreamProducer;

    @GetMapping("/")
    @Operation(summary = "首页接口")
    public String index() {
        redisStreamProducer.sendObjectWithLimit("张三"+ LocalDateTime.now());
        return "index";
    }

}
