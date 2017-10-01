package massage.cs4224c.converter;

import org.junit.Test;

public class WarehouseDistrictTableTest extends AbstractConverterTest {

    public WarehouseDistrictTableTest() {
        super(new WarehouseDistrictTable(), "warehouse_district.csv");
        //this.setGodMod(true);
    }

    @Test
    public void testWarehouseDistrictTable() throws Exception {
        this.testConverter();
    }
}
