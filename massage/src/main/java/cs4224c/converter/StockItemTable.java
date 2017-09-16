package cs4224c.converter;

import cs4224c.util.ProjectConfig;
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

public class StockItemTable extends AbstractConverter {
    private static final int S_W_ID = 0;
    private static final int S_I_ID = 1;
    private static final int S_QUANTITY = 2;
    private static final int S_YTD = 3;
    private static final int S_ORDER_CNT = 4;
    private static final int S_REMOTE_CNT = 5;
    private static final int S_DIST_01 = 6;
    private static final int S_DIST_02 = 7;
    private static final int S_DIST_03 = 8;
    private static final int S_DIST_04 = 9;
    private static final int S_DIST_05 = 10;
    private static final int S_DIST_06 = 11;
    private static final int S_DIST_07 = 12;
    private static final int S_DIST_08 = 13;
    private static final int S_DIST_09 = 14;
    private static final int S_DIST_10 = 15;
    private static final int S_DATA = 16;

    private static final int I_ID = 0;
    private static final int I_NAME = 1;
    private static final int I_PRICE = 2;
    private static final int I_IM_ID = 3;
    private static final int I_DATA = 4;

    public static void main(String[] args) {
        AbstractConverter stockItem = new StockItemTable();
        stockItem.run();
    }

    @Override
    public void massage() throws Exception {
        ProjectConfig config = ProjectConfig.getInstance();

        Reader stockReader = new FileReader(Paths.get(config.getProjectRoot(), config.getDataSourceFolder(), "data-files", "stock.csv").toFile());
        Iterable<CSVRecord> stockRecords = CSVFormat.INFORMIX_UNLOAD_CSV.parse(stockReader);

        Reader itemReader = new FileReader(Paths.get(config.getProjectRoot(), config.getDataSourceFolder(), "data-files", "item.csv").toFile());
        Iterable<CSVRecord> itemRecords = CSVFormat.INFORMIX_UNLOAD_CSV.parse(itemReader);
        Map<String, CSVRecord> items = new HashMap<String, CSVRecord>();
        for (CSVRecord item : itemRecords) {
            items.put(item.get(I_ID), item);
        }

        FileWriter fileWriter = new FileWriter(Paths.get(config.getProjectRoot(), config.getDataDestFolder(), "stock_item.csv").toFile());
        CSVPrinter csvFilePrinter = new CSVPrinter(fileWriter, CSVFormat.INFORMIX_UNLOAD_CSV);

        for (CSVRecord stock : stockRecords) {
            ArrayList<String> result = new ArrayList<>();
            CSVRecord currItem = items.get(stock.get(S_I_ID));

            result.add(stock.get(S_W_ID));
            result.add(stock.get(S_I_ID));
            result.add(currItem.get(I_DATA));
            result.add(currItem.get(I_IM_ID));
            result.add(currItem.get(I_NAME));
            result.add(currItem.get(I_PRICE));
            result.add(stock.get(S_DATA));
            result.add(stock.get(S_DIST_01));
            result.add(stock.get(S_DIST_02));
            result.add(stock.get(S_DIST_03));
            result.add(stock.get(S_DIST_04));
            result.add(stock.get(S_DIST_05));
            result.add(stock.get(S_DIST_06));
            result.add(stock.get(S_DIST_07));
            result.add(stock.get(S_DIST_08));
            result.add(stock.get(S_DIST_09));
            result.add(stock.get(S_DIST_10));
            result.add(stock.get(S_ORDER_CNT));
            result.add(stock.get(S_QUANTITY));
            result.add(stock.get(S_REMOTE_CNT));
            result.add(stock.get(S_YTD));

            csvFilePrinter.printRecord(result);
        }

        fileWriter.flush();
        fileWriter.close();
        csvFilePrinter.close();
    }
}
