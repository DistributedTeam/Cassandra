package cs4224c.util;

public enum PStatement {

    // New Order Transaction
    GET_D_NEXT_O_ID("SELECT d_next_o_id, w_tax, d_tax FROM warehouse_district WHERE d_w_id = ? AND d_id = ?"),
    UPDATE_D_NEXT_O_ID("UPDATE warehouse_district SET d_next_o_id = ? WHERE d_w_id = ? AND d_id = ? IF d_next_o_id = ?"),
    INSERT_ORDER("INSERT INTO order_by_o_id (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) VALUES (?, ?, ?, ?, ?, ?, ?)"),
    GET_STOCK_DIST_1("SELECT s_quantity, i_price, i_name, s_dist_01 FROM stock_item WHERE s_w_id = ? AND s_i_id = ?"),
    GET_STOCK_DIST_2("SELECT s_quantity, i_price, i_name, s_dist_02 FROM stock_item WHERE s_w_id = ? AND s_i_id = ?"),
    GET_STOCK_DIST_3("SELECT s_quantity, i_price, i_name, s_dist_03 FROM stock_item WHERE s_w_id = ? AND s_i_id = ?"),
    GET_STOCK_DIST_4("SELECT s_quantity, i_price, i_name, s_dist_04 FROM stock_item WHERE s_w_id = ? AND s_i_id = ?"),
    GET_STOCK_DIST_5("SELECT s_quantity, i_price, i_name, s_dist_05 FROM stock_item WHERE s_w_id = ? AND s_i_id = ?"),
    GET_STOCK_DIST_6("SELECT s_quantity, i_price, i_name, s_dist_06 FROM stock_item WHERE s_w_id = ? AND s_i_id = ?"),
    GET_STOCK_DIST_7("SELECT s_quantity, i_price, i_name, s_dist_07 FROM stock_item WHERE s_w_id = ? AND s_i_id = ?"),
    GET_STOCK_DIST_8("SELECT s_quantity, i_price, i_name, s_dist_08 FROM stock_item WHERE s_w_id = ? AND s_i_id = ?"),
    GET_STOCK_DIST_9("SELECT s_quantity, i_price, i_name, s_dist_09 FROM stock_item WHERE s_w_id = ? AND s_i_id = ?"),
    GET_STOCK_DIST_10("SELECT s_quantity, i_price, i_name, s_dist_10 FROM stock_item WHERE s_w_id = ? AND s_i_id = ?"),
    UPDATE_STOCK("UPDATE stock_item SET s_quantity = ? WHERE s_w_id = ? AND s_i_id = ? IF s_quantity = ?"),
    INSERT_ORDER_LINE("INSERT INTO order_line_item (ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info, i_name) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"),
    GET_CUSTOMER("SELECT c_discount, c_last, c_credit from customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?"),

    // Payment Transaction
    GET_BALANCE("SELECT c_payment_cnt, c_balance, c_ytd_payment FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?"),
    UPDATE_BALANCE_PAYMENT("UPDATE customer SET c_payment_cnt = ?, c_balance = ?, c_ytd_payment = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ? IF c_payment_cnt = ?");

    private String cql;

    PStatement(String cql) {
        this.cql = cql;
    }

    public String getCql() {
        return this.cql;
    }

}
