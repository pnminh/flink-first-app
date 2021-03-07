package org.example.batch;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.configuration.FluentConfiguration;
import org.h2.jdbcx.JdbcConnectionPool;
import org.h2.tools.Server;

import java.math.BigDecimal;
import java.sql.Types;

public class DatabaseConnectionExample {
    private static final String URL = "jdbc:h2:~/test;AUTO_SERVER=TRUE";
    private static final String USERNAME = "sa";
    private static final String PASSWORD = "";
    private static final String DRIVER_CLASS = "org.h2.Driver";

    public static void main(String[] args) throws Exception {
        Server server = null;
        final JdbcConnectionPool cp = JdbcConnectionPool.create(URL, USERNAME, PASSWORD);
        try {
            server = Server.createWebServer("-webAllowOthers");
            server.start();
            /*try (Connection conn = cp.getConnection(); Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE IF NOT EXISTS MQ_MSG("
                        + "ADDR VARCHAR(1024),"
                        + "MSG_ID VARCHAR(32),"
                        + "MSG TEXT,"
                        + "CREATED_TIME TIMESTAMP ,"
                        + "PRIMARY KEY(MSG_ID)"
                        + ")");
            }*/
            FluentConfiguration configs = Flyway.configure();
            configs.dataSource(URL, USERNAME, PASSWORD);
            Flyway flyway = new Flyway(configs);
            flyway.clean();
            flyway.migrate();

            final ExecutionEnvironment env
                    = ExecutionEnvironment.getExecutionEnvironment();
            JDBCInputFormat jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
                    .setDrivername(DRIVER_CLASS)
                    .setDBUrl(URL)
                    .setUsername(USERNAME)
                    .setPassword(PASSWORD)
                    .setQuery("SELECT Customer, Quantity,Rate from sales_orders")
                    .setRowTypeInfo(new RowTypeInfo(new TypeInformation[]{
                            BasicTypeInfo.STRING_TYPE_INFO,
                            BasicTypeInfo.INT_TYPE_INFO,
                            BasicTypeInfo.BIG_DEC_TYPE_INFO}))
                    .finish();
            //get data
            DataSet<Row> orderRecords = env.createInput(jdbcInputFormat);
            //orderRecords.print();
            //get total value for each customer
            DataSet<Row> customerTotalValues =
                    orderRecords.map(new MapFunction<Row, Tuple2<String, BigDecimal>>() {
                        @Override
                        public Tuple2<String, BigDecimal> map(Row value) throws Exception {
                            return new Tuple2<>((String) value.getField(0),
                                    ((BigDecimal) value.getField(2)).multiply(new BigDecimal((Integer) value.getField(1))));
                        }
                    })//get value for each order
                            .groupBy(0).reduce(new ReduceFunction<Tuple2<String, BigDecimal>>() {
                        @Override
                        public Tuple2<String, BigDecimal> reduce(Tuple2<String, BigDecimal> sum, Tuple2<String, BigDecimal> nextVal) throws Exception {
                            return new Tuple2<>(sum.f0, sum.f1.add(nextVal.f1));
                        }
                    })//get totalValue
                            .map(new MapFunction<Tuple2<String, BigDecimal>, Row>() {
                                @Override
                                public Row map(Tuple2<String, BigDecimal> custTotal) throws Exception {
                                    return Row.of(custTotal.f0, custTotal.f1);
                                }
                            });//map to new Row Dataset in order to insert to db
            //Define Output Format Builder
            JDBCOutputFormat.JDBCOutputFormatBuilder orderOutputBuilder =
                    JDBCOutputFormat.buildJDBCOutputFormat()
                            .setDrivername(DRIVER_CLASS)
                            .setDBUrl(URL)
                            .setUsername(USERNAME)
                            .setPassword(PASSWORD)
                            .setQuery("INSERT INTO customer_summary VALUES (?,?) ")
                            .setSqlTypes(new int[]{Types.VARCHAR, Types.DECIMAL});
            customerTotalValues.output(orderOutputBuilder.finish()).getDataSet().print();
        } finally {
            cp.dispose();
            if (server != null) {
                server.stop();
            }
        }


    }
}
