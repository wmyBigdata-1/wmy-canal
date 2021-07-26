package com.wmy.flink_real_warehouse.canal_client.driver;

import com.wmy.flink_real_warehouse.canal_client.client.CanalClient;

/**
 * ClassName:Entrance
 * Package:com.wmy.flink_real_warehouse.canal_client.driver
 *
 * @date:2021/7/26 13:02
 * @author:数仓开发工程师
 * @email:wmy_2000@163.com
 * @Description: Canal 客户端程序的入口类
 */
public class Entrance {
    public static void main(String[] args) {
        // 1、实例Canal客户端对象，调用start方法，拉取canal server 端的 MySQL bin-log日志
        CanalClient canalClient = new CanalClient();
        canalClient.start();
    }
}
