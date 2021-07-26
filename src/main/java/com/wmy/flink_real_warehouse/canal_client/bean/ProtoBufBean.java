package com.wmy.flink_real_warehouse.canal_client.bean;

/**
 * ClassName:ProtoBufBean
 * Package:com.wmy.flink_real_warehouse.canal_client.bean
 *
 * @date:2021/7/26 13:27
 * @author:数仓开发工程师
 * @email:wmy_2000@163.com
 * @Description: 定义一个ProtoBufBean的序列化接口
 * 这个接口定义返回的byte[]二进制字节码对象
 * 所有的能够使用的protobuf序列化的bean对象都需要集成该接口
 */
public interface ProtoBufBean {
    // 将对象转换二进制数组
    byte[] toBytes();
}
