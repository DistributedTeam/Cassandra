package massage.cs4224c.converter;

import massage.cs4224c.userDefinedType.Address;
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

public class CustomerTable extends AbstractConverter {

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

    public static void main(String[] args) {
        AbstractConverter customer = new CustomerTable();
        customer.run();
    }

    @Override
    public void massage() throws Exception {
        ProjectConfig config = ProjectConfig.getInstance();

        Reader customerReader = new FileReader(Paths.get(config.getProjectRoot(), config.getDataSourceFolder(), "data-files", "customer.csv").toFile());
        Iterable<CSVRecord> customerRecords = CSVFormat.INFORMIX_UNLOAD_CSV.parse(customerReader);

        FileWriter fileWriter = new FileWriter(Paths.get(config.getProjectRoot(), config.getDataDestFolder(), "customer.csv").toFile());
        CSVPrinter csvFilePrinter = new CSVPrinter(fileWriter, CSVFormat.INFORMIX_UNLOAD_CSV);

        for (CSVRecord customer : customerRecords) {
            ArrayList<String> result = new ArrayList<>();

            result.add(customer.get(C_W_ID));
            result.add(customer.get(C_D_ID));
            result.add(customer.get(C_ID));
            result.add(new Address(customer.get(C_STREET_1), customer.get(C_STREET_2), customer.get(C_CITY), customer.get(C_STATE), customer.get(C_ZIP)).toString());
            result.add(customer.get(C_BALANCE));
            result.add(customer.get(C_CREDIT));
            result.add(customer.get(C_CREDIT_LIM));
            result.add(customer.get(C_DATA));
            result.add(customer.get(C_DELIVERY_CNT));
            result.add(customer.get(C_DISCOUNT));
            result.add(customer.get(C_FIRST));
            result.add(customer.get(C_LAST));
            result.add(customer.get(C_MIDDLE));
            result.add(customer.get(C_PAYMENT_CNT));
            result.add(customer.get(C_PHONE));
            result.add(TimeUtility.parse(customer.get(C_SINCE)).toString());
            result.add(customer.get(C_YTD_PAYMENT));

            csvFilePrinter.printRecord(result);
        }

        fileWriter.flush();
        fileWriter.close();
        csvFilePrinter.close();
    }
}
