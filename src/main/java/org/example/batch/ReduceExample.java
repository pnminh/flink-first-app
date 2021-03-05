package org.example.batch;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class ReduceExample {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<OrderModel> pojoInput = env.readCsvFile(Constants.SALES_ORDERS_FILE_INPUT).ignoreFirstLine().parseQuotedStrings('\"')
                .pojoType(OrderModel.class, "id", "customer", "product", "date", "quantity", "rate", "tags");
        //print total charge for each customer
        pojoInput.map(new MapFunction<OrderModel, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(OrderModel value) throws Exception {
                return new Tuple2<>(value.getCustomer(), value.getQuantity() * value.getRate());
            }
        })
                .groupBy(0)
                .aggregate(Aggregations.SUM, 1)
                .print();

        //print total items and total charge per product
        pojoInput.map(new MapFunction<OrderModel, Tuple3<String, Integer, Double>>() {
            @Override
            public Tuple3<String, Integer, Double> map(OrderModel value) throws Exception {
                return new Tuple3<>(value.getProduct(), value.getQuantity(), value.getQuantity() * value.getRate());
            }
        }).groupBy(0)
                .reduce(new ReduceFunction<Tuple3<String, Integer, Double>>() {
                    @Override
                    public Tuple3<String, Integer, Double> reduce(Tuple3<String, Integer, Double> sum, Tuple3<String, Integer, Double> nextItem) throws Exception {
                        return new Tuple3<>(sum.f0, //item to be grouped
                                sum.f1 + nextItem.f1,
                                sum.f2 + nextItem.f2);
                    }
                })
                .print();
    }
}
