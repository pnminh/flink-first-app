package org.example;

import lombok.Data;

@Data
public class OrderWithTotalModel {
    Integer id;
    String customer;
    String product;
    String date;
    Integer quantity;
    Double rate;
    String tags;
    Double total;
}
