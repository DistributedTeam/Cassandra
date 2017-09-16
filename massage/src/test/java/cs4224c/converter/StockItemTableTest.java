package cs4224c.converter;

import org.junit.Test;

public class StockItemTableTest extends AbstractConverterTest {

    public StockItemTableTest() {
        super(new StockItemTable(), "stock_item.csv");
        //this.setGodMod(true);
    }

    @Test
    public void testStockItemTable() throws Exception {
        this.testConverter();
    }
}
