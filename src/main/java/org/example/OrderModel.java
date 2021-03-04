package org.example;

import lombok.Data;

@Data
public class OrderModel {
    Integer id;
    String customer;
    String product;
    String date;
    Integer quantity;
    Double rate;
    String tags;
}
