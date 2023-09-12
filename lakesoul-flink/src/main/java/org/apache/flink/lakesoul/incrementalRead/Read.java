package org.apache.flink.lakesoul.incrementalRead;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
public class Read {

    static String TABLE_NAME = "table.name";
    static String START_TIME = "startTime";
    static String END_TIME = "endTime";
    public static void main(String[] args) {

        ParametersTool parameters = ParametersTool.fromArgs(args);
        String tableName = parameters.get(TABLE_NAME,"test_table");
        String startTime = parameters.get(START_TIME,"2023-09-08 12:00:00");
        String endTime = parameters.get(END_TIME,"2023-12-31 12:00:00");

        // 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        StreamTableEnvironment tEnvs = StreamTableEnvironment.create(env);
        //设置lakesoul 类型的catalog
        Catalog lakesoulCatalog = new LakeSoulCatalog();
        tEnvs.registerCatalog("lakeSoul", lakesoulCatalog);
        tEnvs.useCatalog("lakeSoul");
        tEnvs.useDatabase("default");

        String sqlOps = "/*+ OPTIONS('readtype'='incremental','readstarttime'='{startTime}','readendtime'='{endTime}') */";
        String sqlOps_ = sqlOps.replace("{startTime}",startTime).replace("{endTime}",endTime);

        String sumSql = "SELECT col4,sum(col5),sum(col6),sum(col7) from " +
                tableName +
                 sqlOps_ +
                " group by col4";
        tEnvs.executeSql(sumSql).print();

        String groupBySql = "SELECT col4,max(col5),max(col6),min(col7),count(col16) from " +
                tableName +
                sqlOps_ +
                " group by col4";
        tEnvs.executeSql(groupBySql).print();

        String orderSql = "SELECT * from " +
                tableName+
                sqlOps_ +
                " order by col3,id";
        tEnvs.executeSql(orderSql).print();

        String whereSql = "SELECT * from "+
                tableName +
                sqlOps_ +
                " where id<10000000";
        tEnvs.executeSql(whereSql).print();

        String distinctSql = "SELECT distinct col3 from " +
                tableName +
                sqlOps_;
        tEnvs.executeSql(distinctSql).print();
    }
}
