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
import java.util.HashMap;
import java.util.Map;

public class OrderLineItemTable extends AbstractConverter {
    private static final int OL_W_ID = 0;
    private static final int OL_D_ID = 1;
    private static final int OL_O_ID = 2;
    private static final int OL_NUMBER = 3;
    private static final int OL_I_ID = 4;
    private static final int OL_DELIVERY_D = 5;
    private static final int OL_AMOUNT = 6;
    private static final int OL_SUPPLY_W_ID = 7;
    private static final int OL_QUANTITY = 8;
    private static final int OL_DIST_INFO = 9;

    private static final int I_ID = 0;
    private static final int I_NAME = 1;
    private static final int I_PRICE = 2;
    private static final int I_IM_ID = 3;
    private static final int I_DATA = 4;

    public static void main(String[] args) {
        AbstractConverter orderLineItem = new OrderLineItemTable();
        orderLineItem.run();
    }

    @Override
    public void massage() throws Exception {
        ProjectConfig config = ProjectConfig.getInstance();

        Reader orderLineReader = new FileReader(Paths.get(config.getProjectRoot(), config.getDataSourceFolder(), "data-files", "order-line.csv").toFile());
        Iterable<CSVRecord> orderLineRecords = CSVFormat.INFORMIX_UNLOAD_CSV.parse(orderLineReader);

        Reader itemReader = new FileReader(Paths.get(config.getProjectRoot(), config.getDataSourceFolder(), "data-files", "item.csv").toFile());
        Iterable<CSVRecord> itemRecords = CSVFormat.INFORMIX_UNLOAD_CSV.parse(itemReader);
        Map<String, CSVRecord> items = new HashMap<String, CSVRecord>();
        for (CSVRecord item : itemRecords) {
            items.put(item.get(I_ID), item);
        }


        FileWriter fileWriter = new FileWriter(Paths.get(config.getProjectRoot(), config.getDataDestFolder(), "order_line_item.csv").toFile());
        CSVPrinter csvFilePrinter = new CSVPrinter(fileWriter, CSVFormat.INFORMIX_UNLOAD_CSV);

        for (CSVRecord orderLine : orderLineRecords) {
            ArrayList<String> result = new ArrayList<>();

            result.add(orderLine.get(OL_W_ID));
            result.add(orderLine.get(OL_D_ID));
            result.add(orderLine.get(OL_O_ID));
            result.add(orderLine.get(OL_NUMBER));

            CSVRecord currItem = items.get(orderLine.get(OL_I_ID));
            result.add(currItem.get(I_NAME));

            result.add(orderLine.get(OL_AMOUNT));

            String deliveryD = orderLine.get(OL_DELIVERY_D);
            result.add("null".equals(deliveryD) ? "" : TimeUtility.parse(deliveryD).toString());

            result.add(orderLine.get(OL_DIST_INFO));
            result.add(orderLine.get(OL_I_ID));
            result.add(orderLine.get(OL_QUANTITY));
            result.add(orderLine.get(OL_SUPPLY_W_ID));

            csvFilePrinter.printRecord(result);
        }

        fileWriter.flush();
        fileWriter.close();
        csvFilePrinter.close();
    }
}
