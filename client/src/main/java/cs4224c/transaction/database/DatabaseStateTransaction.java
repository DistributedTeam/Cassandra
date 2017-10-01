package cs4224c.transaction.database;

import com.datastax.driver.core.Row;
import cs4224c.transaction.AbstractTransaction;
import cs4224c.util.QueryExecutor;

public class DatabaseStateTransaction extends AbstractTransaction {

    private static int INDEX_FIRST = 0;

    private static int INDEX_C_BALANCE = 0;
    private static int INDEX_C_YTD = 1;
    private static int INDEX_C_PAYMENT_CNT = 2;
    private static int INDEX_C_DELIVERY_CNT = 3;

    private static int INDEX_O_ID = 0;
    private static int INDEX_O_OL_CNT = 1;

    private static int INDEX_OL_AMOUNT = 0;
    private static int INDEX_OL_QUANTITY = 1;

    private static int INDEX_S_QUANTITY = 0;
    private static int INDEX_S_YTD = 1;
    private static int INDEX_S_ORDER_CNT = 2;
    private static int INDEX_S_REMOTE_CNT = 3;


    @Override
    public void executeFlow() {

        Row customerAggregation = QueryExecutor.getInstance().execute("SELECT sum(c_balance), sum(c_ytd_payment), sum(c_payment_cnt), sum(c_delivery_cnt) FROM customer").one();
        System.out.println(String.format("(a). select sum(W_YTD) from Warehouse : %.4f", customerAggregation.getDouble(INDEX_C_YTD)));
        System.out.println(String.format("(b). select sum(D_YTD), sum(D_NEXT_O_ID) from District : %.4f, %d", customerAggregation.getDouble(INDEX_C_YTD),
                QueryExecutor.getInstance().execute("SELECT count(d_next_o_id) FROM warehouse_district").one().getLong(INDEX_FIRST)));
        System.out.println(String.format("(c). select sum(C_BALANCE), sum(C_YTD_PAYMENT), sum(C_PAYMENT CNT), sum(C_DELIVERY_CNT) from Customer : %.4f, %.4f, %d, %d",
                customerAggregation.getDouble(INDEX_C_BALANCE), customerAggregation.getDouble(INDEX_C_YTD), customerAggregation.getInt(INDEX_C_PAYMENT_CNT), customerAggregation.getInt(INDEX_C_DELIVERY_CNT)));

        Row orderAggregation = QueryExecutor.getInstance().execute("SELECT max(o_id), sum(o_ol_cnt) FROM order_by_o_id").one();
        System.out.println(String.format("(d). select max(O_ID), sum(O_OL_CNT) from Order : %d, %d", orderAggregation.getInt(INDEX_O_ID), orderAggregation.getInt(INDEX_O_OL_CNT)));

        Row orderLineAggregation = QueryExecutor.getInstance().execute("SELECT sum(ol_amount), sum(ol_quantity) FROM order_line_item").one();
        System.out.println(String.format("(e). select sum(OL_AMOUNT), sum(OL_QUANTITY) from Order-Line : %.4f, %d",
                orderLineAggregation.getDouble(INDEX_OL_AMOUNT), orderLineAggregation.getInt(INDEX_OL_QUANTITY)));

        Row stockAggregation = QueryExecutor.getInstance().execute("SELECT sum(s_quantity), sum(s_ytd), sum(s_order_cnt), sum(s_remote_cnt) FROM stock_item").one();
        System.out.println(String.format("(f). select sum(S_QUANTITY), sum(S_YTD), sum(S_ORDER_CNT), sum(S_REMOTE_CNT) from Stock: %d, %.2f, %d, %d",
                stockAggregation.getInt(INDEX_S_QUANTITY), stockAggregation.getDouble(INDEX_S_YTD), stockAggregation.getInt(INDEX_S_ORDER_CNT), stockAggregation.getInt(INDEX_S_REMOTE_CNT)));
    }

}
