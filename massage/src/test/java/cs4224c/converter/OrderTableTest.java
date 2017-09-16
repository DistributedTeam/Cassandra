package cs4224c.converter;

import org.junit.Test;

public class OrderTableTest extends AbstractConverterTest {

    public OrderTableTest() {
        super(new OrderTable(), "order_by_o_id.csv");
        //this.setGodMod(true);
    }

    @Test
    public void testOrderTable() throws Exception {
        this.testConverter();
    }
}
