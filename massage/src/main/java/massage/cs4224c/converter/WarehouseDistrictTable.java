package massage.cs4224c.converter;

import massage.cs4224c.userDefinedType.Address;
import massage.cs4224c.util.ProjectConfig;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.Reader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class WarehouseDistrictTable extends AbstractConverter {

    private static final int D_W_ID = 0;
    private static final int D_ID = 1;
    private static final int D_NAME = 2;
    private static final int D_STREET_1 = 3;
    private static final int D_STREET_2 = 4;
    private static final int D_CITY = 5;
    private static final int D_STATE = 6;
    private static final int D_ZIP = 7;
    private static final int D_TAX = 8;
    private static final int D_YTD = 9;
    private static final int D_NEXT_O_ID = 10;

    private static final int W_ID = 0;
    private static final int W_NAME = 1;
    private static final int W_STREET_1 = 2;
    private static final int W_STREET_2 = 3;
    private static final int W_CITY = 4;
    private static final int W_STATE = 5;
    private static final int W_ZIP = 6;
    private static final int W_TAX = 7;
    private static final int W_YTD = 8;


    public static void main(String[] args) throws Exception {
        AbstractConverter warehouseDistrict = new WarehouseDistrictTable();
        warehouseDistrict.run();
    }

    @Override
    public void massage() throws Exception {
        ProjectConfig config = ProjectConfig.getInstance();

        Reader warehouseReader = new FileReader(Paths.get(config.getProjectRoot(), config.getDataSourceFolder(), "data-files", "warehouse.csv").toFile());
        Iterable<CSVRecord> warehouseRecords = CSVFormat.INFORMIX_UNLOAD_CSV.parse(warehouseReader);
        Map<String, CSVRecord> warehouses = new HashMap<String, CSVRecord>();
        for (CSVRecord warehouse : warehouseRecords) {
            warehouses.put(warehouse.get(W_ID), warehouse);
        }

        Reader districtReader = new FileReader(Paths.get(config.getProjectRoot(), config.getDataSourceFolder(), "data-files", "district.csv").toFile());
        Iterable<CSVRecord> districtRecords = CSVFormat.INFORMIX_UNLOAD_CSV.parse(districtReader);

        FileWriter fileWriter = new FileWriter(Paths.get(config.getProjectRoot(), config.getDataDestFolder(), "warehouse_district.csv").toFile());
        CSVPrinter csvFilePrinter = new CSVPrinter(fileWriter, CSVFormat.INFORMIX_UNLOAD_CSV);

        for (CSVRecord district : districtRecords) {
            ArrayList<String> result = new ArrayList<>();
            result.add(district.get(D_W_ID));
            result.add(district.get(D_ID));

            Address districtAddress = new Address(district.get(D_STREET_1), district.get(D_STREET_2),
                    district.get(D_CITY), district.get(D_STATE), district.get(D_ZIP));
            result.add(districtAddress.toString());

            result.add(district.get(D_NAME));
            result.add(district.get(D_NEXT_O_ID));
            result.add(district.get(D_TAX));
            //result.add(district.get(D_YTD));

            CSVRecord currWarehouse = warehouses.get(result.get(D_W_ID));

            Address currWarehouseAddress = new Address(currWarehouse.get(W_STREET_1), currWarehouse.get(W_STREET_2),
                    currWarehouse.get(W_CITY), currWarehouse.get(W_STATE), currWarehouse.get(W_ZIP));
            result.add(currWarehouseAddress.toString());

            result.add(currWarehouse.get(W_NAME));
            result.add(currWarehouse.get(W_TAX));

            csvFilePrinter.printRecord(result);
        }

        fileWriter.flush();
        fileWriter.close();
        csvFilePrinter.close();
    }
}
