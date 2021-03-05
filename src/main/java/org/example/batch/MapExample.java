package org.example.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.util.Collector;

public class MapExample {


    public static void main(String... args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //Load data
        mapUsingTuple(env);
        mapUsingPojo(env);
        flatMapUsingPojo(env);
    }

    private static void flatMapUsingPojo(ExecutionEnvironment env) throws Exception {
        DataSet<OrderModel> pojoInput = env.readCsvFile(Constants.SALES_ORDERS_FILE_INPUT).ignoreFirstLine().parseQuotedStrings('\"')
                .pojoType(OrderModel.class, "id", "customer", "product", "date", "quantity", "rate", "tags");
        DataSet<OrderModel> orderWithTagListModelDataSet =
                pojoInput.flatMap(
                        new FlatMapFunction<OrderModel, OrderModel>() {
                            @Override
                            public void flatMap(OrderModel orderInput, Collector<OrderModel> collector) throws Exception {
                                for (String tag : orderInput.getTags().split(":")) {
                                    OrderModel outputOrder = OrderModelMapper.INSTANCE.orderToOrder(orderInput);
                                    outputOrder.setTags(tag);
                                    collector.collect(outputOrder);
                                }
                            }
                        }
                );
        orderWithTagListModelDataSet.first(5).print();
    }

    private static DataSet<OrderModel> mapUsingPojo(ExecutionEnvironment env) {
        DataSet<OrderModel> pojoInput = env.readCsvFile(Constants.SALES_ORDERS_FILE_INPUT).ignoreFirstLine().parseQuotedStrings('\"')
                .pojoType(OrderModel.class, "id", "customer", "product", "date", "quantity", "rate", "tags");
        DataSet<OrderWithTotalModel> pojoOutput = pojoInput.map((inputOrder -> {
            OrderWithTotalModel outputOrder = OrderModelMapper.INSTANCE.orderToOrderWithTotal(inputOrder);
            outputOrder.setTotal(inputOrder.getRate() * inputOrder.getQuantity());
            return outputOrder;
        }
        ));
        //pojoOutput.first(5).print();
        return pojoInput;
    }

    private static void mapUsingTuple(ExecutionEnvironment env) {
        DataSet<Tuple7<Integer, String, String, String, Integer, Double, String>> input = env
                .readCsvFile(Constants.SALES_ORDERS_FILE_INPUT)
                .ignoreFirstLine()
                .parseQuotedStrings('\"')//remove " from the input string
                .types(Integer.class, String.class, String.class, String.class,
                        Integer.class, Double.class, String.class);
        DataSet<Tuple8<Integer, String, String, String, Integer, Double, String, Double>> output = input
                .map(
                        //need to use anonymous method due to types removed with lambda
                        new MapFunction<Tuple7<Integer, String, String, String, Integer, Double, String>, Tuple8<Integer, String, String, String, Integer, Double, String, Double>>() {
                            @Override
                            public Tuple8<Integer, String, String, String, Integer, Double, String, Double> map(Tuple7<Integer, String, String, String, Integer, Double, String> tuple7Order) throws Exception {
                                return new Tuple8<>(tuple7Order.f0, tuple7Order.f1,
                                        tuple7Order.f2, tuple7Order.f3, tuple7Order.f4, tuple7Order.f5, tuple7Order.f6, (tuple7Order.f4 * tuple7Order.f5));
                            }
                        });
        //output.first(5).print();
    }
}
