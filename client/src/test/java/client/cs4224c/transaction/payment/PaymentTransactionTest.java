package client.cs4224c.transaction.payment;

import client.cs4224c.parser.PaymentParser;
import client.cs4224c.transaction.BaseTransactionTest;
import client.cs4224c.util.QueryExecutor;
import com.datastax.driver.core.ResultSet;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PaymentTransactionTest extends BaseTransactionTest {

    private final Logger logger = LoggerFactory.getLogger(PaymentTransactionTest.class);


    public PaymentTransactionTest() {
        super(PaymentParser.class);
    }

    @Test
    public void testCase1() throws Exception {
        this.executeFlowWithData("case1.txt");

        logger.info("Begin to validate database for PaymentTransaction");

        ResultSet customer = QueryExecutor.getInstance().execute("SELECT c_balance, c_ytd_payment, c_payment_cnt FROM customer_stats WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 105");
        Assert.assertEquals("Row[-194429, 19442900, 2]", customer.one().toString());

        ResultSet warehouseDistrict = QueryExecutor.getInstance().execute("SELECT d_ytd FROM warehouse_district_stats WHERE d_w_id = 1 AND d_id = 1");
        Assert.assertEquals("Row[3193429]", warehouseDistrict.one().toString());

        logger.info("End: Validate database for PaymentTransaction");

        logger.info("Begin to validate System output");
        validateSystemOutput("expectedCase1.txt", "");
        logger.info("End: Validate System output");
    }
}
