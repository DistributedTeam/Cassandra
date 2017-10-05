package client.cs4224c.transaction.delivery;

import client.cs4224c.parser.DeliveryParser;
import client.cs4224c.transaction.BaseTransactionTest;
import client.cs4224c.util.QueryExecutor;
import com.datastax.driver.core.ResultSet;
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

        //validate updatedMinOrderIdWithNullCarrierId
        ResultSet updatedMinOrderIdWithNullCarrierIdRow1 = QueryExecutor.getInstance().execute("SELECT dt_min_ud_o_id FROM delivery_transaction WHERE dt_w_id = 2 AND dt_d_id = 1");
        Assert.assertEquals("Row[2200]", updatedMinOrderIdWithNullCarrierIdRow1.one().toString());

        ResultSet updatedMinOrderIdWithNullCarrierIdRow2 = QueryExecutor.getInstance().execute("SELECT dt_min_ud_o_id FROM delivery_transaction WHERE dt_w_id = 2 AND dt_d_id = 2");
        Assert.assertEquals("Row[2114]", updatedMinOrderIdWithNullCarrierIdRow2.one().toString());

        ResultSet updatedMinOrderIdWithNullCarrierIdRow3 = QueryExecutor.getInstance().execute("SELECT dt_min_ud_o_id FROM delivery_transaction WHERE dt_w_id = 2 AND dt_d_id = 3");
        Assert.assertEquals("Row[2162]", updatedMinOrderIdWithNullCarrierIdRow3.one().toString());

        ResultSet updatedMinOrderIdWithNullCarrierIdRow4 = QueryExecutor.getInstance().execute("SELECT dt_min_ud_o_id FROM delivery_transaction WHERE dt_w_id = 2 AND dt_d_id = 4");
        Assert.assertEquals("Row[2108]", updatedMinOrderIdWithNullCarrierIdRow4.one().toString());

        ResultSet updatedMinOrderIdWithNullCarrierIdRow5 = QueryExecutor.getInstance().execute("SELECT dt_min_ud_o_id FROM delivery_transaction WHERE dt_w_id = 2 AND dt_d_id = 5");
        Assert.assertEquals("Row[2114]", updatedMinOrderIdWithNullCarrierIdRow5.one().toString());

        ResultSet updatedMinOrderIdWithNullCarrierIdRow6 = QueryExecutor.getInstance().execute("SELECT dt_min_ud_o_id FROM delivery_transaction WHERE dt_w_id = 2 AND dt_d_id = 6");
        Assert.assertEquals("Row[2118]", updatedMinOrderIdWithNullCarrierIdRow6.one().toString());

        ResultSet updatedMinOrderIdWithNullCarrierIdRow7 = QueryExecutor.getInstance().execute("SELECT dt_min_ud_o_id FROM delivery_transaction WHERE dt_w_id = 2 AND dt_d_id = 7");
        Assert.assertEquals("Row[2106]", updatedMinOrderIdWithNullCarrierIdRow7.one().toString());

        ResultSet updatedMinOrderIdWithNullCarrierIdRow8 = QueryExecutor.getInstance().execute("SELECT dt_min_ud_o_id FROM delivery_transaction WHERE dt_w_id = 2 AND dt_d_id = 8");
        Assert.assertEquals("Row[2140]", updatedMinOrderIdWithNullCarrierIdRow8.one().toString());

        ResultSet updatedMinOrderIdWithNullCarrierIdRow9 = QueryExecutor.getInstance().execute("SELECT dt_min_ud_o_id FROM delivery_transaction WHERE dt_w_id = 2 AND dt_d_id = 9");
        Assert.assertEquals("Row[2106]", updatedMinOrderIdWithNullCarrierIdRow9.one().toString());

        ResultSet updatedMinOrderIdWithNullCarrierIdRow = QueryExecutor.getInstance().execute("SELECT dt_min_ud_o_id FROM delivery_transaction WHERE dt_w_id = 2 AND dt_d_id = 10");
        Assert.assertEquals("Row[2140]", updatedMinOrderIdWithNullCarrierIdRow.one().toString());

    }
}