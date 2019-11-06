import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.henry.canal_kafka.util.GlobalConfigUtil;
import com.henry.canal_kafka.util.KafkaSender;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Canal解析binlog日志工具类
 */
public class CanalClient {

    static class ColumnValuePair {
        private String columnName;
        private String columnValue;
        private Boolean isValid;

        public ColumnValuePair(String columnName, String columnValue, Boolean isValid) {
            this.columnName = columnName;
            this.columnValue = columnValue;
            this.isValid = isValid;
        }

        public String getColumnName() { return columnName; }
        public void setColumnName(String columnName) { this.columnName = columnName; }
        public String getColumnValue() { return columnValue; }
        public void setColumnValue(String columnValue) { this.columnValue = columnValue; }
        public Boolean getIsValid() { return isValid; }
        public void setIsValid(Boolean isValid) { this.isValid = isValid; }
    }

    /**
     * 获取Canal连接
     *
     * @param host     主机名
     * @param port     端口号
     * @param instance Canal实例名
     * @param username 用户名
     * @param password 密码
     * @return Canal连接器
     */
    public static CanalConnector getConn(String host, int port, String instance, String username, String password) {
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress(host, port), instance, username, password);

        return canalConnector;
    }

    /**
     * 解析Binlog日志
     *
     * @param entries    Binlog消息实体
     * @param emptyCount 操作的序号
     */
    public static void analysis(List<CanalEntry.Entry> entries, int emptyCount) {
        for (CanalEntry.Entry entry : entries) {
            // 只解析mysql事务的操作，其他的不解析
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN ||
                    entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }

            // 那么解析binlog
            CanalEntry.RowChange rowChange = null;

            try {
                rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                e.printStackTrace();
            }

            // 获取操作类型字段（增加  删除  修改）
            CanalEntry.EventType eventType = rowChange.getEventType();
            // 获取binlog文件名称
            String logfileName = entry.getHeader().getLogfileName();
            // 读取当前操作在binlog文件的位置
            long logfileOffset = entry.getHeader().getLogfileOffset();
            // 获取当前操作所属的数据库
            String dbName = entry.getHeader().getSchemaName();
            // 获取当前操作所属的表
            String tableName = entry.getHeader().getTableName();//当前操作的是哪一张表
            long timestamp = entry.getHeader().getExecuteTime();//执行时间

            // 解析操作的行数据
            for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                // 删除操作
                if (eventType == CanalEntry.EventType.DELETE) {
                    // 获取删除之前的所有列数据
                    dataDetails(rowData.getBeforeColumnsList(), logfileName, logfileOffset, dbName, tableName, eventType, emptyCount,timestamp);
                }
                // 新增操作
                else if (eventType == CanalEntry.EventType.INSERT) {
                    // 获取新增之后的所有列数据
                    dataDetails(rowData.getAfterColumnsList(), logfileName, logfileOffset, dbName, tableName, eventType, emptyCount,timestamp);
                }
                // 更新操作
                else {
                    // 获取更新之后的所有列数据
                    dataDetails(rowData.getAfterColumnsList(), logfileName, logfileOffset, dbName, tableName, eventType, emptyCount,timestamp);
                }
            }
        }
    }

    /**
     * 解析具体一条Binlog消息的数据
     *
     * @param columns       当前行所有的列数据
     * @param logFileName   binlog文件名
     * @param logFileOffset 当前操作在binlog中的位置
     * @param dbName        当前操作所属数据库名称
     * @param tableName     当前操作所属表名称
     * @param eventType     当前操作类型（新增、修改、删除）
     * @param emptyCount    操作的序号
     */
    private static void dataDetails(List<CanalEntry.Column> columns,
                                    String logFileName,
                                    Long logFileOffset,
                                    String dbName,
                                    String tableName,
                                    CanalEntry.EventType eventType,
                                    int emptyCount,
                                    long timestamp) {

        // 找到当前那些列发生了改变  以及改变的值
        List<ColumnValuePair> columnValueList = new ArrayList<ColumnValuePair>();

        for (CanalEntry.Column column : columns) {
            ColumnValuePair columnValuePair = new ColumnValuePair(column.getName(), column.getValue(), column.getUpdated());
            columnValueList.add(columnValuePair);
        }

        String key = UUID.randomUUID().toString();

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("logFileName", logFileName);
        jsonObject.put("logFileOffset", logFileOffset);
        jsonObject.put("dbName", dbName);
        jsonObject.put("tableName", tableName);
        jsonObject.put("eventType", eventType);
        jsonObject.put("columnValueList", columnValueList);
        jsonObject.put("emptyCount", emptyCount);
        jsonObject.put("timestamp", timestamp);


        // 拼接所有binlog解析的字段
        String data = JSON.toJSONString(jsonObject);

        System.out.println(data);

        // 解析后的数据发送到kafka
        KafkaSender.sendMessage(GlobalConfigUtil.kafkaInputTopic, key, data);
    }


    public static void main(String[] args) {

        // 加载配置文件
        String host = GlobalConfigUtil.canalHost;
        int port = Integer.parseInt(GlobalConfigUtil.canalPort);
        String instance = GlobalConfigUtil.canalInstance;
        String username = GlobalConfigUtil.mysqlUsername;
        String password = GlobalConfigUtil.mysqlPassword;

        // 获取Canal连接
        CanalConnector conn = getConn(host, port, instance, username, password);

        // 从binlog中读取数据
        int batchSize = 100;
        int emptyCount = 1;

        try {
            // 连接cannal
            conn.connect();
            //订阅实例中所有的数据库和表
            conn.subscribe(".*\\..*");
            // 回滚到未进行ack的地方
            conn.rollback();

            int totalCount = 120; //循环次数

            while (totalCount > emptyCount) {
                // 获取数据
                Message message = conn.getWithoutAck(batchSize);

                long id = message.getId();
                int size = message.getEntries().size();
                if (id == -1 || size == 0) {
                    //没有读取到任何数据
                } else {
                    //有数据，那么解析binlog日志
                    analysis(message.getEntries(), emptyCount);
                    emptyCount++;
                }

                // 确认消息
                conn.ack(message.getId());

            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            conn.disconnect();
        }
    }
}
