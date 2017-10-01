package massage.cs4224c.converter;

import com.google.common.collect.Lists;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DeliveryTransactionTable extends AbstractConverter {

    private static final int O_W_ID = 0;
    private static final int O_D_ID = 1;
    private static final int O_ID = 2;
    private static final int O_C_ID = 3;
    private static final int O_CARRIER_ID = 4;
    private static final int O_OL_CNT = 5;
    private static final int O_ALL_LOCAL = 6;
    private static final int O_ENTRY_D = 7;

    public static void main(String[] args) {
        AbstractConverter order = new DeliveryTransactionTable();
        order.run();
    }

    @Override
    public void massage() throws Exception {
        ProjectConfig config = ProjectConfig.getInstance();

        Reader orderReader = new FileReader(Paths.get(config.getProjectRoot(), config.getDataSourceFolder(), "data-files", "order.csv").toFile());
        Iterable<CSVRecord> orderRecords = CSVFormat.INFORMIX_UNLOAD_CSV.parse(orderReader);

        FileWriter fileWriter = new FileWriter(Paths.get(config.getProjectRoot(), config.getDataDestFolder(), "delivery_transaction.csv").toFile());
        CSVPrinter csvFilePrinter = new CSVPrinter(fileWriter, CSVFormat.INFORMIX_UNLOAD_CSV);

        Map<List<String>, String> minTable = new HashMap<List<String>, String>();
        for (CSVRecord order : orderRecords) {
            List<String> tuple = Lists.newArrayList(order.get(O_W_ID), order.get(O_D_ID));
            if (!"null".equalsIgnoreCase(order.get(O_CARRIER_ID))) {
                // we only care about null value
                continue;
            }
            if (!minTable.containsKey(tuple)) {
                minTable.put(tuple, order.get(O_ID));
                continue;
            }
            String oldValue = minTable.get(tuple);
            // bigger than the one in the table
            if (oldValue.compareTo(order.get(O_ID)) > 0) {
                minTable.put(tuple, order.get(O_ID));
            }
        }

        for (List<String> key : minTable.keySet()) {
            ArrayList<String> result = new ArrayList<>();

            result.addAll(key);
            result.add(minTable.get(key));

            csvFilePrinter.printRecord(result);
        }

        fileWriter.flush();
        fileWriter.close();
        csvFilePrinter.close();
    }
}
