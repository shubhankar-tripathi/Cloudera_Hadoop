orders = LOAD '/dualcore/orders' AS (order_id:int,
             cust_id:int,
             order_dtm:chararray);

details = LOAD '/dualcore/order_details' AS (order_id:int,
             prod_id:int);


-- TODO (A): Include only records from during the ad campaign
recent = FILTER orders BY order_dtm matches '^2013-05-.*$';


-- TODO (B): Find all the orders containing the advertised product
tablets = FILTER details BY prod_id == 1274348;


-- TODO (C): Get the details for each of those orders
order_contains_tablets = JOIN tablets BY order_id, details BY order_id;


-- TODO (D): Count the number of items in each order
group_by_order = GROUP order_contains_tablets BY tablets::order_id;
--DESCRIBE group_by_order;


-- TODO (E): Calculate the average number of items in each order
order_size = FOREACH group_by_order GENERATE group, COUNT(order_contains_tablets) AS number_of_products;
grouped = GROUP order_size ALL;
average = FOREACH grouped GENERATE AVG(order_size.number_of_products);
-- Display the final result to the screen
DUMP average;
