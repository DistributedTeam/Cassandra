package massage.cs4224c.converter;

import massage.cs4224c.util.ProjectConfig;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.Reader;
import java.nio.file.Paths;
import java.util.ArrayList;


public class CustomerStatsTable extends AbstractConverter {

    private static final int C_W_ID = 0;
    private static final int C_D_ID = 1;
    private static final int C_ID = 2;
    private static final int C_FIRST = 3;
    private static final int C_MIDDLE = 4;
    private static final int C_LAST = 5;
    private static final int C_STREET_1 = 6;
    private static final int C_STREET_2 = 7;
    private static final int C_CITY = 8;
    private static final int C_STATE = 9;
    private static final int C_ZIP = 10;
    private static final int C_PHONE = 11;
    private static final int C_SINCE = 12;
    private static final int C_CREDIT = 13;
    private static final int C_CREDIT_LIM = 14;
    private static final int C_DISCOUNT = 15;
    private static final int C_BALANCE = 16;
    private static final int C_YTD_PAYMENT = 17;
    private static final int C_PAYMENT_CNT = 18;
    private static final int C_DELIVERY_CNT = 19;
    private static final int C_DATA = 20;

    private static final int O_W_ID = 0;
    private static final int O_D_ID = 1;
    private static final int O_ID = 2;
    private static final int O_C_ID = 3;

    public static void main(String[] args) {
        AbstractConverter customer = new CustomerStatsTable();
        customer.run();
    }

    @Override
    public void massage() throws Exception {
        ProjectConfig config = ProjectConfig.getInstance();

        Reader customerReader = new FileReader(Paths.get(config.getProjectRoot(), config.getDataSourceFolder(), "data-files", "customer.csv").toFile());
        Iterable<CSVRecord> customerRecords = CSVFormat.INFORMIX_UNLOAD_CSV.parse(customerReader);

        FileWriter fileWriter = new FileWriter(Paths.get(config.getProjectRoot(), config.getDataDestFolder(), "customer_stats.csv").toFile());
        CSVPrinter csvFilePrinter = new CSVPrinter(fileWriter, CSVFormat.INFORMIX_UNLOAD_CSV);

        for (CSVRecord customer : customerRecords) {
            ArrayList<String> result = new ArrayList<>();

            result.add(customer.get(C_W_ID));
            result.add(customer.get(C_D_ID));
            result.add(customer.get(C_ID));

            result.add(String.valueOf(new Double(Double.parseDouble(customer.get(C_BALANCE)) * 100).longValue())); // DECIMAL(12, 2)
            result.add(customer.get(C_DELIVERY_CNT));
            result.add(customer.get(C_PAYMENT_CNT));
            result.add(String.valueOf(new Double(Double.parseDouble(customer.get(C_YTD_PAYMENT)) * 10000).longValue())); // DECIMAL(12, 4)

            csvFilePrinter.printRecord(result);
        }

        fileWriter.flush();
        fileWriter.close();
        csvFilePrinter.close();
    }
}
