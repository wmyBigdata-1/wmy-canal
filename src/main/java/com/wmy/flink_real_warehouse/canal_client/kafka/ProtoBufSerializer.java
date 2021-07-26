package com.wmy.flink_real_warehouse.canal_client.kafka;

import com.wmy.flink_real_warehouse.canal_client.bean.ProtoBufBean;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * ClassName:ProtoBufSerializer
 * Package:com.wmy.flink_real_warehouse.canal_client.protobuf
 *
 * @date:2021/7/26 13:18
 * @author:数仓开发工程师
 * @email:wmy_2000@163.com
 * @Description: 如何实现一个反序列化的方式，得遵守kafka的序列化方式，如果是其它的序列化的方式的话会报错的
 * 把数据写到kafka里面，包一定不能导错了
 * 泛型是一个传入的对象，既然是一个对象，将这个对象给定义出来，定义成一个接口
 * 实现kafka value的自定义序列化对象，要求传递的参数泛型必须是继承自ProtoBufBean接口的实现类，才可以被序列化成功
 */
public class ProtoBufSerializer implements Serializer<ProtoBufBean> {

    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    public byte[] serialize(String topic, ProtoBufBean data) {
        return data.toBytes(); // 返回的就是一个二进制的数组
    }



    public void close() {

    }
}
