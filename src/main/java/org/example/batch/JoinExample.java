package org.example.batch;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple3;

public class JoinExample {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<OrderModel> orders = env.readCsvFile(Constants.SALES_ORDERS_FILE_INPUT).ignoreFirstLine().parseQuotedStrings('\"')
                .pojoType(OrderModel.class, "id", "customer", "product", "date", "quantity", "rate", "tags");
        DataSet<ProductVendorModel> productVendors = env.readCsvFile(Constants.PRODUCT_VENDOR_FILE_INPUT).ignoreFirstLine().parseQuotedStrings('\"')
                .pojoType(ProductVendorModel.class, "product", "vendor");

        DataSet<Tuple3<String,String,Integer>> vendorProductQuantity = orders
                        .join(productVendors)
                .where("product")
                .equalTo("product")
                .with(new JoinFunction<OrderModel, ProductVendorModel, Tuple3<String,String,Integer>>() {
                    @Override
                    public Tuple3<String, String, Integer> join(OrderModel order, ProductVendorModel productVendor) throws Exception {
                        return new Tuple3<>(productVendor.getVendor(), productVendor.getProduct(),order.getQuantity());
                    }
                });
        vendorProductQuantity.groupBy(0,1)
                .aggregate(Aggregations.SUM, 2)
                .print();// print total quantity for each vendor

    }
}
