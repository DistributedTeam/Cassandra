package massage.cs4224c.converter;

import org.junit.Test;

public class CustomerStatsTableTest extends AbstractConverterTest {

    public CustomerStatsTableTest() {
        super(new CustomerStatsTable(), "customer_stats.csv");
        //this.setGodMod(true);
    }

    @Test
    public void testCustomerTable() throws Exception {
        this.testConverter();
    }
}
