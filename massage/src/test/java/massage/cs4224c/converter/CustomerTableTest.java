package massage.cs4224c.converter;

import org.junit.Test;

public class CustomerTableTest extends AbstractConverterTest {

    public CustomerTableTest() {
        super(new CustomerTable(), "customer.csv");
        //this.setGodMod(true);
    }

    @Test
    public void testCustomerTable() throws Exception {
        this.testConverter();
    }
}
