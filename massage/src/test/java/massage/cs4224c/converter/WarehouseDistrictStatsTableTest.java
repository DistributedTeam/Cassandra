package massage.cs4224c.converter;

import org.junit.Test;

public class WarehouseDistrictStatsTableTest extends AbstractConverterTest {

    public WarehouseDistrictStatsTableTest() {
        super(new WarehouseDistrictStatsTable(), "warehouse_district_stats.csv");
        //this.setGodMod(true);
    }

    @Test
    public void testWarehouseDistrictTable() throws Exception {
        this.testConverter();
    }
}
