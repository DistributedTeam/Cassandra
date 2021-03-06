package massage.cs4224c.converter;

import massage.cs4224c.util.ProjectConfig;
import massage.cs4224c.util.TimeUtility;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.Reader;
import java.nio.file.Paths;
import java.util.ArrayList;

public class OrderTable extends AbstractConverter {

    private static final int O_W_ID = 0;
    private static final int O_D_ID = 1;
    private static final int O_ID = 2;
    private static final int O_C_ID = 3;
    private static final int O_CARRIER_ID = 4;
    private static final int O_OL_CNT = 5;
    private static final int O_ALL_LOCAL = 6;
    private static final int O_ENTRY_D = 7;

    public static void main(String[] args) {
        AbstractConverter order = new OrderTable();
        order.run();
    }

    @Override
    public void massage() throws Exception {
        ProjectConfig config = ProjectConfig.getInstance();

        Reader orderReader = new FileReader(Paths.get(config.getProjectRoot(), config.getDataSourceFolder(), "data-files", "order.csv").toFile());
        Iterable<CSVRecord> orderRecords = CSVFormat.INFORMIX_UNLOAD_CSV.parse(orderReader);

        FileWriter fileWriter = new FileWriter(Paths.get(config.getProjectRoot(), config.getDataDestFolder(), "order_by_o_id.csv").toFile());
        CSVPrinter csvFilePrinter = new CSVPrinter(fileWriter, CSVFormat.INFORMIX_UNLOAD_CSV);

        for (CSVRecord order : orderRecords) {
            ArrayList<String> result = new ArrayList<>();

            result.add(order.get(O_W_ID));
            result.add(order.get(O_D_ID));
            result.add(order.get(O_ID));

            Boolean allLocal = "0".equals(order.get(O_ALL_LOCAL)) ? Boolean.FALSE : Boolean.TRUE;
            result.add(allLocal.toString());

            result.add(order.get(O_C_ID));

            String carrierId = "null".equals(order.get(O_CARRIER_ID)) ? "" : order.get(O_CARRIER_ID);
            result.add(carrierId);

            Long entryD = TimeUtility.parse(order.get(O_ENTRY_D));
            result.add(entryD.toString());

            result.add(order.get(O_OL_CNT));

            csvFilePrinter.printRecord(result);
        }

        fileWriter.flush();
        fileWriter.close();
        csvFilePrinter.close();
    }
}
