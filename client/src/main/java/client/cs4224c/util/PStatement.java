package client.cs4224c.util;

public enum PStatement {

    // New Order Transaction
    GET_D_NEXT_O_ID("SELECT d_next_o_id, w_tax, d_tax FROM warehouse_district WHERE d_w_id = ? AND d_id = ?"),
    UPDATE_D_NEXT_O_ID("UPDATE warehouse_district SET d_next_o_id = ? WHERE d_w_id = ? AND d_id = ? IF d_next_o_id = ?"),
    INSERT_ORDER("INSERT INTO order_by_o_id (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) VALUES (?, ?, ?, ?, ?, ?, ?)"),
    GET_STOCK_DIST_1("SELECT i_price, i_name, s_dist_01 FROM stock_item WHERE s_w_id = ? AND s_i_id = ?"),
    GET_STOCK_DIST_2("SELECT i_price, i_name, s_dist_02 FROM stock_item WHERE s_w_id = ? AND s_i_id = ?"),
    GET_STOCK_DIST_3("SELECT i_price, i_name, s_dist_03 FROM stock_item WHERE s_w_id = ? AND s_i_id = ?"),
    GET_STOCK_DIST_4("SELECT i_price, i_name, s_dist_04 FROM stock_item WHERE s_w_id = ? AND s_i_id = ?"),
    GET_STOCK_DIST_5("SELECT i_price, i_name, s_dist_05 FROM stock_item WHERE s_w_id = ? AND s_i_id = ?"),
    GET_STOCK_DIST_6("SELECT i_price, i_name, s_dist_06 FROM stock_item WHERE s_w_id = ? AND s_i_id = ?"),
    GET_STOCK_DIST_7("SELECT i_price, i_name, s_dist_07 FROM stock_item WHERE s_w_id = ? AND s_i_id = ?"),
    GET_STOCK_DIST_8("SELECT i_price, i_name, s_dist_08 FROM stock_item WHERE s_w_id = ? AND s_i_id = ?"),
    GET_STOCK_DIST_9("SELECT i_price, i_name, s_dist_09 FROM stock_item WHERE s_w_id = ? AND s_i_id = ?"),
    GET_STOCK_DIST_10("SELECT s_quantity, i_price, i_name, s_dist_10 FROM stock_item WHERE s_w_id = ? AND s_i_id = ?"),
    GET_STOCK("SELECT s_quantity, s_ytd, s_order_cnt, s_remote_cnt FROM stock_item WHERE s_w_id = ? AND s_i_id = ?"),
    UPDATE_STOCK("UPDATE stock_item SET s_quantity = ?, s_ytd = ?, s_order_cnt = ?, s_remote_cnt = ? WHERE s_w_id = ? AND s_i_id = ? IF s_order_cnt = ?"),
    INSERT_ORDER_LINE("INSERT INTO order_line_item (ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info, i_name) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"),
    GET_CUSTOMER("SELECT c_discount, c_last, c_credit from customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?"),
    UPDATE_CUSTOMER_LAST_ORDER("UPDATE customer SET c_last_order = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ? IF c_last_order <= ?"),

    // Payment Transaction
    GET_BALANCE_PAYMENT("SELECT c_balance, c_ytd_payment, c_payment_cnt FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?"),
    UPDATE_BALANCE_PAYMENT("UPDATE customer SET c_balance = ?, c_ytd_payment = ?, c_payment_cnt = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ? IF c_payment_cnt = ?"),
    GET_CUSTOMER_FULL("SELECT c_first, c_middle, c_last, c_address, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?"),
    GET_WD_ADDRESS("SELECT w_address, d_address FROM warehouse_district WHERE d_w_id = ? AND d_id = ?"),

    //Delivery Transaction
    GET_MIN_ORDER_ID_WITH_NULL_CARRIER_ID("SELECT dt_min_ud_o_id FROM delivery_transaction WHERE dt_w_id = ? AND dt_d_id = ?"),
    UPDATE_MIN_ORDER_ID_WITH_NULL_CARRIER_ID("UPDATE delivery_transaction SET dt_min_ud_o_id = ? WHERE dt_w_id = ? AND dt_d_id = ? IF dt_min_ud_o_id = ?"),
    GET_CUSTOMER_ID_FROM_ORDER("SELECT o_c_id FROM order_by_o_id WHERE o_w_id = ? AND o_d_id = ? and o_id = ?"),
    UPDATE_ORDER_CARRIER_ID("UPDATE order_by_o_id SET o_carrier_id = ? WHERE o_w_id = ? AND o_d_id = ? and o_id = ?"),
    GET_ORDER_LINES("SELECT ol_number, ol_amount FROM order_line_item WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?"),
    UPDATE_ORDER_LINES_DELIVERY_DATE("UPDATE order_line_item SET ol_delivery_d = ? WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? and ol_number = ?"),
    GET_CUSTOMER_BALANCE_AND_DELIVERY_COUNT("SELECT c_balance, c_delivery_cnt FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?"),
    UPDATE_CUSTOMER_BALANCE_AND_DELIVERY_COUNT("UPDATE customer SET c_balance = ?, c_delivery_cnt = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ? IF c_delivery_cnt = ?"),

    // OrderStatus Transaction
    GET_CUSTOMER_NAME_AND_BALANCE("SELECT c_first, c_middle, c_last, c_balance, c_last_order from customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?"),
    GET_CUSTOMER_LAST_ORDER("SELECT o_entry_d, o_carrier_id FROM order_by_o_id WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?"),
    GET_ORDER_LINES_FOR_LAST_ORDER("SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM order_line_item WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?"),

    // Stock-Level Transaction
    GET_NEXT_ORDER_NUMBER("SELECT d_next_o_id FROM warehouse_district WHERE d_w_id = ? AND d_id = ?"),
    GET_LAST_L_ORDER_ITEMS("SELECT ol_i_id FROM order_line_item WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?"),
    GET_STOCK_QUANTITY("SELECT s_quantity FROM stock_item WHERE s_w_id = ? AND s_i_id = ?"),

    // Top Balance Transaction
    GET_W_ID_D_ID_W_NAME_D_NAME("SELECT d_w_id, d_id, w_name, d_name FROM warehouse_district"),
    GET_TOP_BALANCE_CUSTOMER("SELECT c_first, c_middle, c_last, c_balance FROM customer_balance WHERE c_w_id = ? AND c_d_id = ? ORDER BY c_balance DESC LIMIT 10");
    
    private String cql;

    PStatement(String cql) {
        this.cql = cql;
    }

    public String getCql() {
        return this.cql;
    }

}
