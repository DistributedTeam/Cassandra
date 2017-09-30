package cs4224c.transaction.payment;

import cs4224c.parser.PaymentParser;
import cs4224c.transaction.BaseTransactionTest;
import org.junit.Test;

public class PaymentTransactionTest extends BaseTransactionTest {

    protected PaymentTransactionTest() {
        super(PaymentParser.class);
    }

    @Test
    public void testCase1() throws Exception {
        this.executeFlowWithData("case1.txt");
    }
}
