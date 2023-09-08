package org.apache.flink.lakesoul.incrementalRead;

import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
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

        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        Catalog lakesoulCatalog = new LakeSoulCatalog();

        tEnv.registerCatalog("lakeSoul",lakesoulCatalog);
        tEnv.useCatalog("lakeSoul");
        tEnv.useDatabase("default");

        String sqlOps = "/*+ OPTIONS('readtype'='incremental','readstarttime'='{startTime}','readendtime'='{endTime}') */";
        String sqlOps_ = sqlOps.replace("{startTime}",startTime).replace("{endTime}",endTime);

        String sumSql = "SELECT col4,sum(col5),sum(col6),sum(col7) from " +
                tableName +
                 sqlOps_ +
                " group by col4";
        tEnv.executeSql(sumSql).print();

        String groupBySql = "SELECT col4,max(col5),max(col6),min(col7),count(col16) from " +
                tableName +
                sqlOps_ +
                " group by col4";
        tEnv.executeSql(groupBySql).print();

        String orderSql = "SELECT * from " +
                tableName+
                sqlOps_ +
                " order by col3,id";
        tEnv.executeSql(orderSql).print();

        String whereSql = "SELECT * from "+
                tableName +
                sqlOps_ +
                " where id<10000000";
        tEnv.executeSql(whereSql).print();

        String distinctSql = "SELECT distinct col3 from " +
                tableName +
                sqlOps_;
        tEnv.executeSql(distinctSql).print();
    }
}
