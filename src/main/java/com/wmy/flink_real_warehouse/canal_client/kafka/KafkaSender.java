package com.wmy.flink_real_warehouse.canal_client.kafka;

import com.alibaba.fastjson.JSON;
import com.wmy.flink_real_warehouse.canal_client.bean.CanalRowData;
import com.wmy.flink_real_warehouse.canal_client.util.ConfigUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;
import java.util.Properties;

/**
 * ClassName:KafkaSender
 * Package:com.wmy.flink_real_warehouse.canal_client.kafka
 *
 * @date:2021/7/26 14:30
 * @author:数仓开发工程师
 * @email:wmy_2000@163.com
 * @Description: kafka生产者对象
 */
public class KafkaSender {
    // 1、首先定义kafka生者工具类
    private Properties kafkaProps = new Properties();

    // 定义生产者对象，value使用的是自定义的序列化的方式，该序列化的方式要求传递的是一个ProtoBufbean的子类
    private KafkaProducer<String, CanalRowData> kafkaProducer;
    private KafkaProducer<String, String> kafkaProducer1;

    // 2、初始化kafka的生产者对象
    public KafkaSender() {
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigUtil.kafkaBootstrap_servers_config());
        kafkaProps.put(ProducerConfig.BATCH_SIZE_CONFIG, ConfigUtil.kafkaBatch_size_config());
        kafkaProps.put(ProducerConfig.ACKS_CONFIG, ConfigUtil.kafkaAcks());
        kafkaProps.put(ProducerConfig.RETRIES_CONFIG, ConfigUtil.kafkaRetries());
        kafkaProps.put(ProducerConfig.CLIENT_ID_CONFIG, ConfigUtil.kafkaClient_id_config());
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ConfigUtil.kafkaKey_serializer_class_config());
//        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ConfigUtil.kafkaValue_serializer_class_config());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ConfigUtil.kafkaKey_serializer_class_config());

        // 实例化生产者对象
        kafkaProducer = new KafkaProducer<String, CanalRowData>(kafkaProps);
        kafkaProducer1 = new KafkaProducer<String, String>(kafkaProps);
    }

    // 传递数据：将数据写入kafka集群
    public void send(CanalRowData canalRowData) {
        kafkaProducer.send(new ProducerRecord<String, CanalRowData>(ConfigUtil.kafkaTopic(), canalRowData));
    }

    public void send(Map map) {
        System.out.println(JSON.toJSON(map).toString());
        kafkaProducer1.send(new ProducerRecord<String, String>(ConfigUtil.kafkaTopic(), JSON.toJSON(map).toString()));
    }
}
