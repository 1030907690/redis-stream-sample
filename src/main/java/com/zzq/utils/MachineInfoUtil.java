package com.zzq.utils;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * 电脑信息
 * @author: Zhou Zhongqing
 * @since : 5/28/2026 10:08 PM
 */
public class MachineInfoUtil {

    private static final Logger log = LoggerFactory.getLogger(MachineInfoUtil.class);

    public static String getHostName() {
        try {
            // 获取本机的InetAddress对象
            InetAddress localHost = InetAddress.getLocalHost();
            // 获取主机名
            return localHost.getHostName();
        } catch (UnknownHostException e) {
            log.error("获取主机名失败 {} ",e.getMessage());
            throw new RuntimeException(e.getMessage());
        }
    }

}
