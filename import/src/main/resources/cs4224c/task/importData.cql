COPY warehouse_district FROM '_DATA_CSV_FOLDER_warehouse_district.csv' WITH DELIMITER = ',' AND HEADER = FALSE;
COPY customer FROM '_DATA_CSV_FOLDER_customer.csv' WITH DELIMITER = ',' AND HEADER = FALSE;
COPY order_by_o_id FROM '_DATA_CSV_FOLDER_order_by_o_id.csv' WITH DELIMITER = ',' AND HEADER = FALSE;
COPY order_line_item FROM '_DATA_CSV_FOLDER_order_line_item.csv' WITH DELIMITER = ',' AND HEADER = FALSE;
COPY stock_item FROM '_DATA_CSV_FOLDER_stock_item.csv' WITH DELIMITER = ',' AND HEADER = FALSE;