package com.wmy.flink_real_warehouse.canal_client.bean;

import com.alibaba.fastjson.JSON;
import com.google.protobuf.InvalidProtocolBufferException;
import com.wmy.flink_real_warehouse.canal_client.kafka.CanalModel;
import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

/**
 * ClassName:CanalRowData
 * Package:com.wmy.flink_real_warehouse.canal_client.bean
 *
 * @date:2021/7/26 13:58
 * @author:数仓开发工程师
 * @email:wmy_2000@163.com
 * @Description: 这个是Canal 数据的protobuf数据的实现类
 * 能够使用protobuf序列化bean对象，将binlog的解析对象转换成protobuf序列化的字节码的字节码数据，最终写入kafka的topic中
 */
@Setter
@Getter
public class CanalRowData implements ProtoBufBean{

    // 需要解析的参数
    private String logfileName;
    private Long logfileOffset;
    private Long executeTime;
    private String schemaName;
    private String tableName;
    private String eventType;

    // 列的集合
    private Map<String, String> map;

    private Map<String, String> columns;

    // 把解析对象给传进来 ---> 解析binlog日志
    public CanalRowData(Map map) {
        // 解析map对象的所有参数
        if (map.size() > 0) { // 这个不用判断也行，因为在拉取binlog的时候就已经是判断好了的
            this.logfileName = map.get("logfileName").toString();
            this.logfileOffset = Long.parseLong(map.get("logfileOffset").toString());
            this.executeTime = Long.parseLong(map.get("executeTime").toString());
            this.schemaName = map.get("schemaName").toString();
            this.tableName = map.get("tableName").toString();
            this.eventType = map.get("eventType").toString();
            this.columns = (Map<String, String>) map.get("columns");
        }
    }

    // 需要将map对象解析出来的参数，赋值给protobuf对象，然后序列化后字节码返回
    public byte[] toBytes() {
        CanalModel.RowData.Builder builder = CanalModel.RowData.newBuilder();
        builder.setLogfilename(this.getLogfileName());
        builder.setLogfileoffset(this.getLogfileOffset());
        builder.setExecuteTime(this.getExecuteTime());
        builder.setSchemaName(this.getSchemaName());
        builder.setTableName(this.getTableName());
        builder.setEventType(this.getEventType());
        for (String key : this.getColumns().keySet()) {
            builder.putColumns(key, this.getColumns().get(key));
        }
        //将传递的binlog数据解析后序列化成字节码数据返回
        return builder.build().toByteArray();
    }


    // 传递一个字节码数据，将字节码数据反序列化成对象
    public CanalRowData(byte[] bytes){
        try {
            //将字节码数据反序列成对象
            CanalModel.RowData rowData = CanalModel.RowData.parseFrom(bytes);
            this.logfileName = rowData.getLogfilename();
            this.logfileOffset = rowData.getLogfileoffset();
            this.executeTime = rowData.getExecuteTime();
            this.schemaName = rowData.getSchemaName();
            this.tableName = rowData.getTableName();
            this.eventType = rowData.getEventType();

            //将所有的列的集合添加到map中
            this.columns = new HashMap<>();
            this.columns.putAll(rowData.getColumnsMap());
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }

    // 将map类型转换为json对象
    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
