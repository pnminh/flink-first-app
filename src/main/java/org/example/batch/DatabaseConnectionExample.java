package org.example.batch;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.configuration.FluentConfiguration;
import org.h2.jdbcx.JdbcConnectionPool;
import org.h2.tools.Server;

import java.sql.SQLException;

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
            orderRecords.print();
        } finally {
            cp.dispose();
            if (server != null) {
                server.stop();
            }
        }


    }
}
