package org.example.batch;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import scala.Tuple3;

import java.util.HashMap;
import java.util.Map;

public class BroadcastExample {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<OrderModel> input = env.readCsvFile(Constants.SALES_ORDERS_FILE_INPUT).ignoreFirstLine().parseQuotedStrings('\"')
                .pojoType(OrderModel.class, "id", "customer", "product", "date", "quantity", "rate", "tags");
        Map<String,Double> productDiscounts = new HashMap<>();
        productDiscounts.put("Mouse", 0.05);
        productDiscounts.put("Keyboard", 0.10);
        productDiscounts.put("Webcam",0.075);
        productDiscounts.put("Headset",0.10);
        DataSet<Map<String, Double>> discountDataset = env.fromElements(productDiscounts);
        DataSet<Tuple3<Integer, Double, Double>> orderTotalBeforeAndAfterDiscounts =
                input.map(new RichMapFunction<OrderModel, Tuple3<Integer, Double, Double>>() {
                    private Map<String, Double> discountRates;

                    @Override
                    public Tuple3<Integer, Double, Double> map(OrderModel value) throws Exception {
                        return new Tuple3<>(value.getId(),
                                value.getQuantity() * value.getRate(),
                                value.getQuantity() * value.getRate() * (1-this.discountRates.get(value.getProduct())));
                    }

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        this.discountRates = (Map<String, Double>) this.getRuntimeContext().getBroadcastVariable("bcDiscountRates").get(0);
                    }
                }).withBroadcastSet(discountDataset, "bcDiscountRates"); //dataset that is available to all running tasks
        orderTotalBeforeAndAfterDiscounts.first(5).print();
    }
}
