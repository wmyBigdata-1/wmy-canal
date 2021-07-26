package com.wmy.canal.client;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ClassName:canal_demo
 * Package:com.wmy.canal.client
 *
 * @date:2021/7/25 17:15
 * @author:数仓开发工程师
 * @email:wmy_2000@163.com
 * @Description: canal是CS架构，server是不需要写代码的，直接部署到linux服务器上就可以了
 * 客户端需要开发，连接服务器段，与canal服务器进行交互
 */
public class canal_demo {
    public static void main(String[] args) {
        /**
         * 开发步骤：
         *  1、创建连接
         *  2、建立连接
         *  3、订阅主题
         *  4、获取数据
         *  5、递交确定
         *  6、关闭连接
         */
        // 1、创建连接
        CanalConnector connector1 = CanalConnectors
                .newSingleConnector(
                        new InetSocketAddress(
                                "yaxin01", 11111),
                        "example", "", "");
        // 服务端
        CanalConnector connector = CanalConnectors
                .newClusterConnector(
                        "yaxin01:2181,yaxin02:2181", // 生成环境中最好是写完整
                        "example",
                        "canal", // 用户名，这个是随便写的
                        "canal"
                );

        // 定义一个不停拉取数据的标记
        boolean isRunning = true;

        // 拉取数据的数量的批次
        int batchSize = 1000;

        try {
            // 2、建立连接
            connector.connect();
            // 设置回滚一次的get请求，重写获取数据
            connector.rollback();

            // 3、订阅主题
            connector.subscribe("itcast_shop.*");

            // 4、获取数据 循环获取
            while (isRunning) {
                Message message = connector.getWithoutAck(batchSize); // batchSize 条数据的对象
                // 获取批次的ID
                long batchID = message.getId();
                // 获取binlog的日志总数
                int size = message.getEntries().size();
                if (size == 0 || size == -1) {
                    // 没有获取到数据，不需要处理数据
                } else {
                    // 测试1：拉取到数据，对数据进行处理
                    // printSummary(message);

                    // 测试2：转换为JSON对象
                    //String json = binlogToJson(message);
                    //System.out.println(json);

                    // kafka 可以写入的类型 ---> 字符串类型、二进制字节码数据
                    // 因为存储到kafka的数据格式的话，需要占用太多的存储资源和网络资源
                    // 所以需要将存储的数据格式化成占用资源比较小的数据存储格式
                    // java ---> protobuf存储格式
                    // 测试3：protobuf存储格式，
                    // 1、将message对象转换成protobuf的字节码数据
                    // 2、将数据写入到kafka集群中
                    byte[] bytes = binlogToProtoBuf(message);
                    for (byte aByte : bytes) {
                        System.out.println(aByte);
                    }

                }
                // 5、提交确认
                connector.ack(batchID);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //解析获取到的message对象
    private static void printSummary(Message message) {
        // 遍历整个batch中的每个binlog实体
        //entry就是mysql中的一条记录
        for (CanalEntry.Entry entry : message.getEntries()) {
            // 事务开始，对于事务的开始和结束，不需要进行处理，因为处理的是数据本身
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN
                    || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }

            //获取到的数据RowData
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
            String eventTypeName = entry.getHeader().getEventType().toString().toLowerCase();

            System.out.println("logfileName" + ":" + logfileName);
            System.out.println("logfileOffset" + ":" + logfileOffset);
            System.out.println("executeTime" + ":" + executeTime);
            System.out.println("schemaName" + ":" + schemaName);
            System.out.println("tableName" + ":" + tableName);
            System.out.println("eventTypeName" + ":" + eventTypeName);

            CanalEntry.RowChange rowChange = null;

            try {
                // 获取存储数据，并将二进制字节数据解析为RowChange实体
                rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (InvalidProtocolBufferException e) {
                //解析数据的时候会抛出一个ProtocolBufferException异常，很明显这个数据是protobuf的数据格式
                //所以可以确定，canal中传递的数据就是potobuf格式数据
                e.printStackTrace();
            }

            // 迭代每一条变更数据
            for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                // 判断是否为删除事件
                if (entry.getHeader().getEventType() == CanalEntry.EventType.DELETE) {
                    System.out.println("---delete---");
                    printColumnList(rowData.getBeforeColumnsList());
                    System.out.println("---");
                }
                // 判断是否为更新事件
                else if (entry.getHeader().getEventType() == CanalEntry.EventType.UPDATE) {
                    System.out.println("---update---");
                    printColumnList(rowData.getBeforeColumnsList());
                    System.out.println("---");
                    printColumnList(rowData.getAfterColumnsList());
                }
                // 判断是否为插入事件
                else if (entry.getHeader().getEventType() == CanalEntry.EventType.INSERT) {
                    System.out.println("---insert---");
                    printColumnList(rowData.getAfterColumnsList());
                    System.out.println("---");
                }
            }
        }
    }

    // binlog解析为json字符串
    private static String binlogToJson(Message message) throws InvalidProtocolBufferException {
        // 1. 创建Map结构保存最终解析的数据
        Map rowDataMap = new HashMap<String, Object>();

        // 2. 遍历message中的所有binlog实体
        for (CanalEntry.Entry entry : message.getEntries()) {
            // 只处理事务型binlog
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN ||
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

            // 封装列数据
            Map columnDataMap = new HashMap<String, Object>();
            // 获取所有行上的变更
            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            List<CanalEntry.RowData> columnDataList = rowChange.getRowDatasList();
            for (CanalEntry.RowData rowData : columnDataList) {
                if (eventType.equals("insert") || eventType.equals("update")) {
                    for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                        columnDataMap.put(column.getName(), column.getValue());
                    }
                } else if (eventType.equals("delete")) {
                    for (CanalEntry.Column column : rowData.getBeforeColumnsList()) {
                        columnDataMap.put(column.getName(), column.getValue());
                    }
                }
            }

            rowDataMap.put("columns", columnDataMap);
        }

        return JSON.toJSONString(rowDataMap);
    }

    // binlog解析为ProtoBuf
    private static byte[] binlogToProtoBuf(Message message) throws InvalidProtocolBufferException {
        // 1. 构建CanalModel.RowData实体
        CanalModel.RowData.Builder rowDataBuilder = CanalModel.RowData.newBuilder();

        // 1. 遍历message中的所有binlog实体
        for (CanalEntry.Entry entry : message.getEntries()) {
            // 只处理事务型binlog
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN ||
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

            rowDataBuilder.setLogfilename(logfileName);
            rowDataBuilder.setLogfileoffset(logfileOffset);
            rowDataBuilder.setExecuteTime(executeTime);
            rowDataBuilder.setSchemaName(schemaName);
            rowDataBuilder.setTableName(tableName);
            rowDataBuilder.setEventType(eventType);

            // 获取所有行上的变更
            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            List<CanalEntry.RowData> columnDataList = rowChange.getRowDatasList();
            for (CanalEntry.RowData rowData : columnDataList) {
                if (eventType.equals("insert") || eventType.equals("update")) {
                    for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                        rowDataBuilder.putColumns(column.getName(), column.getValue().toString());
                    }
                } else if (eventType.equals("delete")) {
                    for (CanalEntry.Column column : rowData.getBeforeColumnsList()) {
                        rowDataBuilder.putColumns(column.getName(), column.getValue().toString());
                    }
                }
            }
        }

        return rowDataBuilder.build().toByteArray();
    }

    // 打印所有列名和列值
    private static void printColumnList(List<CanalEntry.Column> columnList) {
        //遍历的是一条数据的所有的列
        for (CanalEntry.Column column : columnList) {
            System.out.println(column.getName() + "\t" + column.getValue());
        }
    }
}
