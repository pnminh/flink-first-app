CREATE TABLE `customer_summary` (
  `Customer` VARCHAR(8) DEFAULT NULL,
  `Total_Value` NUMERIC(11,2) DEFAULT NULL
);

CREATE TABLE sales_orders(
    ID       INTEGER  NOT NULL PRIMARY KEY
    ,Customer VARCHAR(8) NOT NULL
    ,Product  VARCHAR(8) NOT NULL
    ,Date     VARCHAR(10) NOT NULL
    ,Quantity INTEGER  NOT NULL
    ,Rate     NUMERIC(5,2) NOT NULL
    ,Tags     VARCHAR(22)
);
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (1,'Apple','Keyboard','2019/11/21',5,31.15,'Discount:Urgent');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (2,'LinkedIn','Headset','2019/11/25',5,36.9,'Urgent:Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (3,'Facebook','Keyboard','2019/11/24',5,49.89,NULL);
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (4,'Google','Webcam','2019/11/07',4,34.21,'Discount');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (5,'LinkedIn','Webcam','2019/11/21',3,48.69,'Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (6,'Google','Mouse','2019/11/23',5,40.58,NULL);
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (7,'LinkedIn','Webcam','2019/11/20',4,37.19,NULL);
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (8,'Google','Mouse','2019/11/13',1,46.79,'Urgent:Discount:Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (9,'Google','Webcam','2019/11/10',4,27.48,'Discount:Urgent');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (10,'LinkedIn','Headset','2019/11/09',2,26.91,'Urgent:Discount:Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (11,'Facebook','Headset','2019/11/26',5,45.84,'Urgent:Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (12,'Google','Headset','2019/11/05',2,41.17,'Discount:Urgent');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (13,'Facebook','Keyboard','2019/11/10',3,31.32,'Urgent:Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (14,'Apple','Mouse','2019/11/09',4,40.27,'Discount');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (15,'Apple','Mouse','2019/11/25',5,38.89,NULL);
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (16,'Facebook','Keyboard','2019/11/09',1,26.37,'Urgent:Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (17,'Apple','Headset','2019/11/09',4,29.98,'Discount:Urgent');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (18,'LinkedIn','Webcam','2019/11/06',3,40.59,'Discount');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (19,'LinkedIn','Webcam','2019/11/06',2,43.92,'Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (20,'LinkedIn','Mouse','2019/11/25',4,36.77,'Urgent:Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (21,'Google','Headset','2019/11/26',3,46.61,'Discount');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (22,'Facebook','Keyboard','2019/11/19',5,37.72,'Discount');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (23,'Facebook','Webcam','2019/11/13',2,28.71,'Urgent');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (24,'Facebook','Headset','2019/11/01',1,36.0,'Discount:Urgent');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (25,'Apple','Webcam','2019/11/03',3,44.95,'Urgent:Discount:Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (26,'LinkedIn','Mouse','2019/11/21',4,31.82,'Discount:Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (27,'LinkedIn','Mouse','2019/11/01',5,26.25,'Discount:Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (28,'LinkedIn','Webcam','2019/11/05',1,38.04,'Discount');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (29,'LinkedIn','Mouse','2019/11/23',1,42.16,'Urgent:Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (30,'Facebook','Webcam','2019/11/16',3,31.38,'Urgent:Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (31,'Google','Keyboard','2019/11/06',4,29.04,NULL);
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (32,'Apple','Mouse','2019/11/11',1,46.43,'Urgent');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (33,'Apple','Headset','2019/11/17',5,49.99,'Urgent:Discount:Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (34,'Facebook','Webcam','2019/11/05',5,42.49,'Urgent:Discount:Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (35,'Google','Mouse','2019/11/17',2,49.33,'Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (36,'LinkedIn','Keyboard','2019/11/22',2,47.45,'Discount:Urgent');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (37,'LinkedIn','Mouse','2019/11/11',4,48.35,'Discount:Urgent');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (38,'Facebook','Keyboard','2019/11/13',5,30.74,'Discount:Urgent');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (39,'Facebook','Keyboard','2019/11/16',5,49.04,'Discount');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (40,'Google','Keyboard','2019/11/01',5,44.11,'Urgent:Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (41,'LinkedIn','Mouse','2019/11/09',1,28.94,'Discount:Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (42,'LinkedIn','Webcam','2019/11/24',3,31.85,'Discount:Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (43,'Apple','Mouse','2019/11/25',2,38.88,'Discount');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (44,'LinkedIn','Webcam','2019/11/08',3,42.47,'Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (45,'Apple','Webcam','2019/11/01',2,44.95,'Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (46,'Google','Keyboard','2019/11/01',2,49.51,'Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (47,'Google','Headset','2019/11/18',4,46.7,'Discount:Urgent');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (48,'Facebook','Mouse','2019/11/12',2,48.81,'Discount');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (49,'Apple','Headset','2019/11/09',3,30.63,'Discount:Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (50,'Facebook','Mouse','2019/11/02',5,33.13,'Urgent');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (51,'Google','Mouse','2019/11/27',4,32.8,'Urgent');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (52,'Google','Keyboard','2019/11/26',2,41.01,NULL);
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (53,'LinkedIn','Headset','2019/11/21',4,32.81,'Discount:Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (54,'LinkedIn','Headset','2019/11/16',4,49.46,'Urgent:Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (55,'Google','Webcam','2019/11/25',3,33.87,'Urgent:Discount:Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (56,'LinkedIn','Headset','2019/11/10',2,39.69,'Urgent:Discount:Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (57,'Google','Mouse','2019/11/21',5,32.0,'Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (58,'Apple','Keyboard','2019/11/24',4,38.1,'Urgent:Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (59,'Apple','Webcam','2019/11/02',3,45.0,'Discount:Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (60,'LinkedIn','Headset','2019/11/08',4,42.51,NULL);
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (61,'Apple','Webcam','2019/11/24',3,42.66,NULL);
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (62,'Google','Keyboard','2019/11/18',1,46.95,'Discount');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (63,'LinkedIn','Mouse','2019/11/25',5,40.74,'Urgent:Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (64,'Apple','Keyboard','2019/11/04',4,44.23,'Discount:Urgent');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (65,'Facebook','Keyboard','2019/11/07',3,49.55,'Discount:Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (66,'LinkedIn','Headset','2019/11/14',5,25.78,'Discount:Urgent');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (67,'Facebook','Mouse','2019/11/26',3,27.36,'Urgent:Discount:Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (68,'LinkedIn','Webcam','2019/11/17',1,48.54,'Urgent:Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (69,'Google','Keyboard','2019/11/28',2,31.5,'Urgent');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (70,'Google','Headset','2019/11/01',3,49.14,'Discount:Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (71,'Google','Mouse','2019/11/29',2,25.35,'Urgent:Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (72,'Apple','Mouse','2019/11/13',1,48.8,'Urgent:Discount:Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (73,'Facebook','Headset','2019/11/24',3,49.67,'Urgent');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (74,'Facebook','Mouse','2019/11/12',4,35.35,'Urgent:Discount:Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (75,'Google','Mouse','2019/11/04',1,28.35,NULL);
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (76,'LinkedIn','Mouse','2019/11/26',5,28.0,'Urgent');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (77,'Apple','Headset','2019/11/17',4,42.89,'Urgent:Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (78,'Apple','Keyboard','2019/11/02',1,38.41,'Urgent:Discount:Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (79,'Facebook','Mouse','2019/11/08',3,47.06,'Discount:Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (80,'Facebook','Mouse','2019/11/05',5,37.22,'Urgent:Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (81,'LinkedIn','Keyboard','2019/11/13',5,32.93,NULL);
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (82,'Apple','Webcam','2019/11/02',4,27.36,'Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (83,'Facebook','Keyboard','2019/11/21',1,44.05,'Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (84,'Facebook','Headset','2019/11/21',4,46.74,'Urgent:Discount:Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (85,'Google','Webcam','2019/11/04',2,36.25,'Discount:Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (86,'LinkedIn','Webcam','2019/11/27',5,40.45,'Discount');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (87,'Google','Mouse','2019/11/27',4,40.95,'Discount:Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (88,'Apple','Webcam','2019/11/22',5,47.84,'Urgent');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (89,'Google','Headset','2019/11/17',3,29.93,'Urgent:Discount:Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (90,'LinkedIn','Headset','2019/11/21',2,44.28,NULL);
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (91,'Google','Keyboard','2019/11/28',3,25.09,'Urgent:Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (92,'Facebook','Headset','2019/11/11',5,31.09,NULL);
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (93,'Apple','Headset','2019/11/08',4,32.17,NULL);
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (94,'LinkedIn','Webcam','2019/11/21',4,47.5,'Discount:Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (95,'Google','Mouse','2019/11/13',1,48.62,'Urgent');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (96,'Facebook','Mouse','2019/11/09',2,43.94,'Discount:Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (97,'Apple','Webcam','2019/11/01',2,47.78,'Discount');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (98,'Google','Mouse','2019/11/22',5,42.07,'Urgent');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (99,'Apple','Webcam','2019/11/13',5,48.84,'Discount:Pickup');
INSERT INTO sales_orders(ID,Customer,Product,Date,Quantity,Rate,Tags) VALUES (100,'Google','Webcam','2019/11/10',5,39.34,'Urgent:Pickup');