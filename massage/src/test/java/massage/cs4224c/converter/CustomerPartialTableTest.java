package massage.cs4224c.converter;

import org.junit.Test;

public class CustomerPartialTableTest extends AbstractConverterTest {

    public CustomerPartialTableTest() {
        super(new CustomerPartialTable(), "customer_partial.csv");
        //this.setGodMod(true);
    }

    @Test
    public void testCustomerTable() throws Exception {
        this.testConverter();
    }
}
