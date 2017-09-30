package cs4224c.converter;

import org.junit.Test;

public class DeliveryTransactionTableTest extends AbstractConverterTest {

    public DeliveryTransactionTableTest() {
        super(new DeliveryTransactionTable(), "delivery_transaction.csv");
        //this.setGodMod(true);
    }

    @Test
    public void testOrderTable() throws Exception {
        this.testConverter();
    }
}
