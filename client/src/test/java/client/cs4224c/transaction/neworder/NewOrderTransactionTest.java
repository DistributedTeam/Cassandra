package client.cs4224c.transaction.neworder;

import com.datastax.driver.core.ResultSet;
import client.cs4224c.parser.NewOrderParser;
import client.cs4224c.transaction.BaseTransactionTest;
import client.cs4224c.util.QueryExecutor;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NewOrderTransactionTest extends BaseTransactionTest {

    private final Logger logger = LoggerFactory.getLogger(NewOrderTransactionTest.class);

    public NewOrderTransactionTest() {
        super(NewOrderParser.class);
    }

    @Test
    public void testCase1() throws Exception {
        this.executeFlowWithData("case1.txt");

        logger.info("Begin to validate database for NewOrderTransaction");

        // validate d_next_o_id
        ResultSet d_next_o_id = QueryExecutor.getInstance().execute("SELECT d_next_o_id FROM warehouse_district WHERE d_w_id = 12 AND d_id = 7");
        Assert.assertEquals("Row[3002]", d_next_o_id.one().toString());

        // validate order
        ResultSet order = QueryExecutor.getInstance().execute("SELECT o_c_id, o_carrier_id, o_ol_cnt, o_all_local FROM order_by_o_id WHERE o_w_id = 12 AND o_d_id = 7 AND o_id = 3001");
        Assert.assertEquals("Row[1120, NULL, 3, false]", order.one().toString());

        // validate customer last order
        ResultSet customerLastOrder = QueryExecutor.getInstance().execute("SELECT c_last_order FROM customer WHERE c_w_id = 12 AND c_d_id = 7 AND c_id = 1120");
        Assert.assertEquals("Row[3001]", customerLastOrder.one().toString());

        // validate stock
        ResultSet stock1 = QueryExecutor.getInstance().execute("SELECT s_quantity, s_ytd, s_order_cnt, s_remote_cnt FROM stock_item WHERE s_w_id = 12 AND s_i_id = 68");
        Assert.assertEquals("Row[10, 15.0, 1, 0]", stock1.one().toString());
        ResultSet stock2 = QueryExecutor.getInstance().execute("SELECT s_quantity, s_ytd, s_order_cnt, s_remote_cnt FROM stock_item WHERE s_w_id = 2 AND s_i_id = 231");
        Assert.assertEquals("Row[109, 90.0, 1, 1]", stock2.one().toString());
        ResultSet stock3 = QueryExecutor.getInstance().execute("SELECT s_quantity, s_ytd, s_order_cnt, s_remote_cnt FROM stock_item WHERE s_w_id = 7 AND s_i_id = 254");
        Assert.assertEquals("Row[66, 10.0, 1, 1]", stock3.one().toString());

        // validate order-lines
        ResultSet orderLines = QueryExecutor.getInstance().execute("SELECT ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_delivery_d, ol_dist_info FROM order_line_item WHERE ol_w_id = 12 AND ol_d_id = 7 AND ol_o_id = 3001");
        Assert.assertEquals("Row[1, 68, 12, 15, NULL, tvlowdfxhiugmrdowiickja]", orderLines.one().toString());
        Assert.assertEquals("Row[2, 231, 2, 90, NULL, ojpfjjhqmucpktqcqmbehpn]", orderLines.one().toString());
        Assert.assertEquals("Row[3, 254, 7, 10, NULL, hasjvglprigsbqmuojnzxrv]", orderLines.one().toString());

        logger.info("End: Validate database for NewOrderTransaction");

        logger.info("Begin to validate System output");

        this.validateSystemOutput("expectedCase1.txt");

        logger.info("End: Validate System output");
    }
}
