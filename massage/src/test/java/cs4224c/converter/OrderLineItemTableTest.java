package cs4224c.converter;

import org.junit.Test;

public class OrderLineItemTableTest extends AbstractConverterTest {

    public OrderLineItemTableTest() {
        super(new OrderLineItemTable(), "order_line_item.csv");
        //this.setGodMod(true);
    }

    @Test
    public void testOrderLineItemTable() throws Exception {
        this.testConverter();
    }
}
