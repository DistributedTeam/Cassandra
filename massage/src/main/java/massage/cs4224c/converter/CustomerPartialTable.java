package massage.cs4224c.converter;

import massage.cs4224c.util.ProjectConfig;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.Reader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public class CustomerPartialTable extends AbstractConverter {

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
        AbstractConverter customer = new CustomerPartialTable();
        customer.run();
    }

    @Override
    public void massage() throws Exception {
        ProjectConfig config = ProjectConfig.getInstance();

        Reader customerReader = new FileReader(Paths.get(config.getProjectRoot(), config.getDataSourceFolder(), "data-files", "customer.csv").toFile());
        Iterable<CSVRecord> customerRecords = CSVFormat.INFORMIX_UNLOAD_CSV.parse(customerReader);

        Reader orderReader = new FileReader(Paths.get(config.getProjectRoot(), config.getDataSourceFolder(), "data-files", "order.csv").toFile());
        Iterable<CSVRecord> orderRecords = CSVFormat.INFORMIX_UNLOAD_CSV.parse(orderReader);

        // pre-process to get customer's last order
        Map<Triple<String, String, String>, String> customerLatestOrder = new HashMap<Triple<String, String, String>, String>();
        for (CSVRecord order : orderRecords) {
            Triple<String, String, String> identifier = new ImmutableTriple<>(order.get(O_W_ID), order.get(O_D_ID), order.get(O_C_ID));
            if (!customerLatestOrder.containsKey(identifier)) {
                customerLatestOrder.put(identifier, order.get(O_ID));
            }
            String O_C_ID = customerLatestOrder.get(identifier);
            if (O_C_ID.compareTo(order.get(O_ID)) < 0) { // use bigger order id
                customerLatestOrder.put(identifier, order.get(O_ID));
            }
        }

        FileWriter fileWriter = new FileWriter(Paths.get(config.getProjectRoot(), config.getDataDestFolder(), "customer_partial.csv").toFile());
        CSVPrinter csvFilePrinter = new CSVPrinter(fileWriter, CSVFormat.INFORMIX_UNLOAD_CSV);

        for (CSVRecord customer : customerRecords) {
            ArrayList<String> result = new ArrayList<>();

            result.add(customer.get(C_W_ID));
            result.add(customer.get(C_D_ID));
            result.add(customer.get(C_ID));

            result.add(customer.get(C_BALANCE));
            result.add(customer.get(C_FIRST));
            result.add(customer.get(C_LAST));

            Triple<String, String, String> identifier = new ImmutableTriple<>(customer.get(C_W_ID), customer.get(C_D_ID), customer.get(C_ID));
            result.add(customerLatestOrder.getOrDefault(identifier, ""));

            result.add(customer.get(C_MIDDLE));

            csvFilePrinter.printRecord(result);
        }

        fileWriter.flush();
        fileWriter.close();
        csvFilePrinter.close();
    }
}
