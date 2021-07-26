package com.wmy.canal.client;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * ClassName:ProtoBufDemo
 * Package:com.wmy.canal.client
 *
 * @date:2021/7/25 18:25
 * @author:数仓开发工程师
 * @email:wmy_2000@163.com
 * @Description: 数据的序列化和反序列
 */
public class ProtoBufDemo {
    public static void main(String[] args) throws InvalidProtocolBufferException {
        // 实例化ProtoBuf对象
        DemoModel.User.Builder builder = DemoModel.User.newBuilder();

        // 给以user对象进行一个赋值操作
        builder.setId(1);
        builder.setName("吴明洋");
        builder.setSex("男");

        // 取值：获取user对象的属性
        // 可以打印，对象是可以放到里面去的
        DemoModel.User userBuilder = builder.build();
        System.out.println(userBuilder.getId());
        System.out.println(userBuilder.getName());
        System.out.println(userBuilder.getSex());

        // 序列化和反序列化
        // 序列化可以将对象转换成字符串字节码对象存储到kafka
        // 反序列化可以将数据kafka中的数据反序列化到内存当中进行使用
        // 将一个对象序列化成一个二进制存储到kafka
        byte[] bytes = builder.build().toByteArray();
        for (byte aByte : bytes) {
            System.out.println(aByte);
        }

        // 从kafka中旬消费出来的序列化数据，可以反序列化成对象使用
        DemoModel.User user = DemoModel.User.parseFrom(bytes);
        System.out.println(user.getId());
        System.out.println(user.getName());
        System.out.println(user.getSex());
    }
}
