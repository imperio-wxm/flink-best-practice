package com.wxmimperio.flink.table;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

public class BatchExe {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(batchEnv);

        DataSet<BatchExe.MyData> csvInput = batchEnv.readCsvFile("D:\\d_backup\\github\\flink-best-practice\\flink-simple-demo\\src\\main\\resources\\test.csv")
                .pojoType(BatchExe.MyData.class, "characterId", "groupId");
        Table batchTable = tEnv.fromDataSet(csvInput);
        tEnv.registerTable("mydata", batchTable);

        DataSet<MyData> csvInput1 = batchEnv.readCsvFile("D:\\d_backup\\github\\flink-best-practice\\flink-simple-demo\\src\\main\\resources\\test1.csv")
                .pojoType(MyData.class, "characterId", "groupId");
        Table table1 = tEnv.fromDataSet(csvInput1);
        tEnv.registerTable("mydata1", table1);

        String sql = "select * from mydata inner join mydata1 on mydata.characterId = mydata1.characterId and mydata.characterId = '2'";

        Table query = tEnv.sqlQuery(sql);

        DataSet<Row> result = tEnv.toDataSet(query, Row.class);
        result.print();
    }

    public static class MyData {
        private String characterId;
        private int groupId;

        public String getCharacterId() {
            return characterId;
        }

        public void setCharacterId(String characterId) {
            this.characterId = characterId;
        }

        public int getGroupId() {
            return groupId;
        }

        public void setGroupId(int groupId) {
            this.groupId = groupId;
        }

        @Override
        public String toString() {
            return "MyData{" +
                    "characterId='" + characterId + '\'' +
                    ", groupId=" + groupId +
                    '}';
        }
    }
}
