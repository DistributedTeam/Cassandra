package client.cs4224c.transaction.delivery;

import client.cs4224c.parser.DeliveryParser;
import client.cs4224c.transaction.BaseTransactionTest;
import client.cs4224c.util.QueryExecutor;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeliveryTransactionTest extends BaseTransactionTest{

    private final Logger logger = LoggerFactory.getLogger(DeliveryTransactionTest.class);

    public DeliveryTransactionTest() {super(DeliveryParser.class);}

    @Test
    public void test1() throws Exception {
        this.executeFlowWithData("deliveryTransactionCase1.txt");

        // validate updatedMinOrderIdWithNullCarrierId
        ResultSet updatedMinOrderIdWithNullCarrierIdRow1 = QueryExecutor.getInstance().execute("SELECT dt_min_ud_o_id FROM delivery_transaction WHERE dt_w_id = 7 AND dt_d_id = 2");
        Assert.assertEquals("Row[2595]", updatedMinOrderIdWithNullCarrierIdRow1.one().toString());

        ResultSet updatedMinOrderIdWithNullCarrierIdRow2 = QueryExecutor.getInstance().execute("SELECT dt_min_ud_o_id FROM delivery_transaction WHERE dt_w_id = 7 AND dt_d_id = 7");
        Assert.assertEquals("Row[2544]", updatedMinOrderIdWithNullCarrierIdRow2.one().toString());

        ResultSet updatedMinOrderIdWithNullCarrierIdRow3 = QueryExecutor.getInstance().execute("SELECT dt_min_ud_o_id FROM delivery_transaction WHERE dt_w_id = 7 AND dt_d_id = 9");
        Assert.assertEquals("Row[2442]", updatedMinOrderIdWithNullCarrierIdRow3.one().toString());

        // validate order carrier_id
        ResultSet orderRow1 = QueryExecutor.getInstance().execute("SELECT o_carrier_id FROM order_by_o_id WHERE o_w_id = 7 AND o_d_id = 2 AND o_id = 2594");
        Assert.assertEquals("Row[3]", orderRow1.one().toString());

        ResultSet orderRow2 = QueryExecutor.getInstance().execute("SELECT o_carrier_id FROM order_by_o_id WHERE o_w_id = 7 AND o_d_id = 7 AND o_id = 2543");
        Assert.assertEquals("Row[3]", orderRow2.one().toString());

        ResultSet orderRow3 = QueryExecutor.getInstance().execute("SELECT o_carrier_id FROM order_by_o_id WHERE o_w_id = 7 AND o_d_id = 9 AND o_id = 2441");
        Assert.assertEquals("Row[3]", orderRow3.one().toString());

        // validate order-lines OL_DELIVERY_D
        ResultSet orderLines1 = QueryExecutor.getInstance().execute("SELECT ol_delivery_d FROM order_line_item WHERE ol_w_id = 7 AND ol_d_id = 2 AND ol_o_id = 2594");
        for (Row orderLine : orderLines1) {
            Assert.assertNotEquals("Row[NULL]", orderLine.toString());
        }

        ResultSet orderLines2 = QueryExecutor.getInstance().execute("SELECT ol_delivery_d FROM order_line_item WHERE ol_w_id = 7 AND ol_d_id = 7 AND ol_o_id = 2543");
        for (Row orderLine : orderLines2) {
            Assert.assertNotEquals("Row[NULL]", orderLine.toString());
        }

        ResultSet orderLines3 = QueryExecutor.getInstance().execute("SELECT ol_delivery_d FROM order_line_item WHERE ol_w_id = 7 AND ol_d_id = 9 AND ol_o_id = 2441");
        for (Row orderLine : orderLines3) {
            Assert.assertNotEquals("Row[NULL]", orderLine.toString());
        }

        // validate customer
        ResultSet customerRow1 = QueryExecutor.getInstance().execute("SELECT c_balance, c_delivery_cnt FROM customer WHERE c_w_id = 7 AND c_d_id = 2 AND c_id = 2816");
        Assert.assertEquals("Row[1011.35, 1]", customerRow1.one().toString());

        ResultSet customerRow2 = QueryExecutor.getInstance().execute("SELECT c_balance, c_delivery_cnt FROM customer WHERE c_w_id = 7 AND c_d_id = 7 AND c_id = 2410");
        Assert.assertEquals("Row[5409.29, 1]", customerRow2.one().toString());

        ResultSet customerRow3 = QueryExecutor.getInstance().execute("SELECT c_balance, c_delivery_cnt FROM customer WHERE c_w_id = 7 AND c_d_id = 9 AND c_id = 2043");
        Assert.assertEquals("Row[1230.71, 1]", customerRow3.one().toString());
    }
}