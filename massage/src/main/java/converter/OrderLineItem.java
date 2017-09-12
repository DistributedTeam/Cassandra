package converter;

import com.google.common.collect.Lists;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import util.Config;
import util.TimeUtility;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.Reader;
import java.nio.file.Paths;
import java.util.ArrayList;

public class OrderLineItem extends AbstractConverter {
    private static final int ID_OFFSET = 1;

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
        AbstractConverter orderLineItem = new OrderLineItem();
        orderLineItem.run();
    }

    @Override
    public void massage() throws Exception {
        Reader orderLineReader = new FileReader(Paths.get(Config.getProjectRoot(), "project-files", "data-files", "order-line.csv").toFile());
        Iterable<CSVRecord> orderLineRecords = CSVFormat.EXCEL.parse(orderLineReader);

        Reader itemReader = new FileReader(Paths.get(Config.getProjectRoot(), "project-files", "data-files", "item.csv").toFile());
        Iterable<CSVRecord> itemRecords = CSVFormat.EXCEL.parse(itemReader);
        ArrayList<CSVRecord> itemList = Lists.newArrayList(itemRecords);

        FileWriter fileWriter = new FileWriter(Paths.get(Config.getProjectRoot(), "database-data", "order_line_item.csv").toFile());
        CSVPrinter csvFilePrinter = new CSVPrinter(fileWriter, CSVFormat.EXCEL);

        for (CSVRecord orderLine : orderLineRecords) {
            ArrayList<String> result = new ArrayList<>();

            result.add(orderLine.get(OL_W_ID));
            result.add(orderLine.get(OL_D_ID));
            result.add(orderLine.get(OL_O_ID));
            result.add(orderLine.get(OL_NUMBER));

            CSVRecord currItem = itemList.get(Integer.parseInt(orderLine.get(OL_I_ID)) - ID_OFFSET);
            result.add(currItem.get(I_NAME));

            result.add(orderLine.get(OL_AMOUNT));

            String deliveryD = orderLine.get(OL_DELIVERY_D);
            result.add("null".equals(deliveryD) ? "0" : TimeUtility.parse(deliveryD).toString());

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
