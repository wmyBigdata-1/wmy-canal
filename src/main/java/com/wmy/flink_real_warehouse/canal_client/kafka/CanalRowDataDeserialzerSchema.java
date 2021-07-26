package com.wmy.flink_real_warehouse.canal_client.kafka;

import com.wmy.flink_real_warehouse.canal_client.bean.CanalRowData;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

import java.io.IOException;

/**
 * ClassName:CanalRowDataDeserialzerSchema
 * Package:com.wmy.flink_real_warehouse.canal_client.kafka
 *
 * @date:2021/7/26 16:28
 * @author:数仓开发工程师
 * @email:wmy_2000@163.com
 * @Description: 自定义反序列化实现类，继承AbstractDeserializationSchema抽象类
 */
public class CanalRowDataDeserialzerSchema extends AbstractDeserializationSchema<CanalRowData> {
    // 将kafka读取到的字节码数据转换成CanalRowData对象返回
    @Override
    public CanalRowData deserialize(byte[] message) throws IOException {
        //需要将二进制的字节码数据转换成CanalRowData对象返回
        return new CanalRowData(message);
    }
}
