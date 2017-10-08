package massage.cs4224c.converter;

import org.junit.Test;

public class StockItemStatsTableTest extends AbstractConverterTest {

    public StockItemStatsTableTest() {
        super(new StockItemStatsTable(), "stock_item_stats.csv");
        //this.setGodMod(true);
    }

    @Test
    public void testStockItemTable() throws Exception {
        this.testConverter();
    }
}
