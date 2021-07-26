package com.wmy.flink_real_warehouse.canal_client.driver;

import com.wmy.flink_real_warehouse.canal_client.bean.CanalRowData;
import com.wmy.flink_real_warehouse.canal_client.kafka.CanalRowDataDeserialzerSchema;
import com.wmy.flink_real_warehouse.canal_client.util.ConfigUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * ClassName:FlinkReadKafkaProtobuf
 * Package:com.wmy.flink_real_warehouse.canal_client.driver
 *
 * @date:2021/7/26 16:25
 * @author:数仓开发工程师
 * @email:wmy_2000@163.com
 * @Description: flink读取kafka中的数据
 */
public class FlinkReadKafkaProtobuf {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "yaxin01:9092,yaxin02:9092,yaxin03:9092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "flinkreadkafkaprotobuf");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        FlinkKafkaConsumer<CanalRowData> kafkaConsumer = new FlinkKafkaConsumer<>(
                "ods_wmy_shop_mysql",
                new CanalRowDataDeserialzerSchema(),
                props
        );

        env.addSource(kafkaConsumer).print();

        env.execute();
    }
}
