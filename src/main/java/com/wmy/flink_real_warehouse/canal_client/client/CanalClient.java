package com.wmy.flink_real_warehouse.canal_client.client;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import com.wmy.flink_real_warehouse.canal_client.bean.CanalRowData;
import com.wmy.flink_real_warehouse.canal_client.kafka.KafkaSender;
import com.wmy.flink_real_warehouse.canal_client.util.ConfigUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ClassName:CanalClient
 * Package:com.wmy.flink_real_warehouse.canal_client.client
 *
 * @date:2021/7/26 13:00
 * @author:数仓开发工程师
 * @email:wmy_2000@163.com
 * @Description: Canal客户端程序，与Canal服务端建立连接，然后获取canal server 端的 MySQL binlog 日志
 */
public class CanalClient {
    // 定义canal客户端的连接器
    private CanalConnector canalConnector;
    // 定义拉取bin-log数据的条数
    private static final int BATCHSIZE = 2 * 1024;
    // 读取canal的配置，定义kafka生产者的工具类
    // 定义kafka生产工具类
    private KafkaSender kafkaSender;

    // 1、构造方法
    public CanalClient() {
        // 1.1 创建连接对象，初始化连接
        canalConnector = CanalConnectors.newClusterConnector(
                ConfigUtil.zookeeperServerIp(),
                ConfigUtil.canalServerDestination(),
                ConfigUtil.canalServerUsername(),
                ConfigUtil.canalServerPassword()); // 用户名和密码是不需要填写的，但是填写的内容是可以随便写的
        // 1.2 实例化kafka生产者的工具类
        kafkaSender = new KafkaSender();
    }

    // 2、开始执行 ---> 实行业务逻辑
    public void start() {
        // 2.1 建立连接
        canalConnector.connect();

        // 2.2 回滚上一次的get请求，重写获取数据
        canalConnector.rollback();

        // 2.3 订阅匹配的数据库
        canalConnector.subscribe(ConfigUtil.canalSubscribeFilter());

        try {
            // 2.4 不停的循环的拉取数据
            while (true) {
                // 2.4.1 拉取binlog日志，每一次拉取 BATCHSIZE
                Message message = canalConnector.getWithoutAck(BATCHSIZE);

                // 2.4.2 获取batchID
                long id = message.getId();

                // 2.4.3 获取binlog的条数
                int size = message.getEntries().size();
                if (size == 0 || size == -1) {
                    // 表示是没有拉取到数据的
                } else {
                    // 2.4.4 将binlog日志解析成binlog Map对象
                    Map binlogMessageToMap = binlogMessageToMap(message);

                    // 2.4.5 不能往kafka存储的map对象，需要将map对象序列化成一个protobuf格式写入到kafka中
                    // 首先得定义protobuf格式的对象，里面采用了一个设计模式
                    //CanalRowData canalRowData = new CanalRowData(binlogMessageToMap);
                    // 打印测试
                    //System.out.println(canalRowData);

                    // 2.4.6 将数据写入到kafka中
                    if (binlogMessageToMap.size() > 0) {
                        // 有数据，将数据发送到kafka集群
                        kafkaSender.send(binlogMessageToMap);
                    }

                }
            }
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        } finally {
            // 2.5 断开连接
            canalConnector.disconnect();
        }
    }

    /**
     * 将binlog日志转换为Map结构
     * @param message
     * @return
     */
    private Map binlogMessageToMap(Message message) throws InvalidProtocolBufferException {
        Map rowDataMap = new HashMap();

        // 1. 遍历message中的所有binlog实体
        for (CanalEntry.Entry entry : message.getEntries()) {
            // 只处理事务型binlog
            if(entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN ||
                    entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }

            // 获取binlog文件名
            String logfileName = entry.getHeader().getLogfileName();
            // 获取logfile的偏移量
            long logfileOffset = entry.getHeader().getLogfileOffset();
            // 获取sql语句执行时间戳
            long executeTime = entry.getHeader().getExecuteTime();
            // 获取数据库名
            String schemaName = entry.getHeader().getSchemaName();
            // 获取表名
            String tableName = entry.getHeader().getTableName();
            // 获取事件类型 insert/update/delete
            String eventType = entry.getHeader().getEventType().toString().toLowerCase();

            rowDataMap.put("logfileName", logfileName);
            rowDataMap.put("logfileOffset", logfileOffset);
            rowDataMap.put("executeTime", executeTime);
            rowDataMap.put("schemaName", schemaName);
            rowDataMap.put("tableName", tableName);
            rowDataMap.put("eventType", eventType);

            // 获取所有行上的变更
            Map<String, String> columnDataMap = new HashMap<String, String>();
            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            List<CanalEntry.RowData> columnDataList = rowChange.getRowDatasList();
            for (CanalEntry.RowData rowData : columnDataList) {
                if(eventType.equals("insert") || eventType.equals("update")) {
                    for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                        columnDataMap.put(column.getName(), column.getValue().toString());
                    }
                }
                else if(eventType.equals("delete")) {
                    for (CanalEntry.Column column : rowData.getBeforeColumnsList()) {
                        columnDataMap.put(column.getName(), column.getValue().toString());
                    }
                }
            }

            rowDataMap.put("columns", columnDataMap);
        }
        return rowDataMap;
    }
}
