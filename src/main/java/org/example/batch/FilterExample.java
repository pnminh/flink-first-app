package org.example.batch;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.text.SimpleDateFormat;
import java.util.Date;

public class FilterExample {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<OrderModel> input = env.readCsvFile(Constants.SALES_ORDERS_FILE_INPUT).ignoreFirstLine().parseQuotedStrings('\"')
                .pojoType(OrderModel.class, "id", "customer", "product", "date", "quantity", "rate", "tags");
        System.out.println("input count: " + input.count());
        DataSet<OrderModel> output = input.filter(order -> {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy/MM/dd");
            Date orderDate = simpleDateFormat.parse(order.getDate());
            return orderDate.compareTo(simpleDateFormat.parse("2019/11/01")) >= 0
                    && orderDate.compareTo(simpleDateFormat.parse("2019/11/11")) < 0;
        });
        System.out.println("output count:" + output.count());


    }

}
