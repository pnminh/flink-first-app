package org.example;

import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.factory.Mappers;

@Mapper
public interface OrderModelMapper {
    OrderModelMapper INSTANCE = Mappers.getMapper(OrderModelMapper.class);
    OrderWithTotalModel orderToOrderWithTotal(OrderModel orderModel);
    OrderModel orderToOrder(OrderModel orderModel);
}
