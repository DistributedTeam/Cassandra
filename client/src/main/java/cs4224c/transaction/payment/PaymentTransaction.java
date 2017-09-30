package cs4224c.transaction.payment;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.UDTValue;
import com.google.common.collect.Lists;
import cs4224c.transaction.AbstractTransaction;
import cs4224c.transaction.payment.data.PaymentTransactionData;
import cs4224c.util.PStatement;
import cs4224c.util.QueryExecutor;
import cs4224c.util.TimeUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PaymentTransaction extends AbstractTransaction {

    private final Logger logger = LoggerFactory.getLogger(PaymentTransaction.class);

    private PaymentTransactionData data;

    public PaymentTransactionData getData() {
        return data;
    }

    public void setData(PaymentTransactionData data) {
        this.data = data;
    }

    private static int INDEX_C_BALANCE = 0;
    private static int INDEX_C_YTD = 1;
    private static int INDEX_C_CNT = 2;

    private static int INDEX_C_FIRST = 0;
    private static int INDEX_C_MIDDLE = 1;
    private static int INDEX_C_LAST = 2;
    private static int INDEX_C_ADDRESS = 3;
    private static int INDEX_C_PHONE = 4;
    private static int INDEX_C_SINCE = 5;
    private static int INDEX_C_CREDIT = 6;
    private static int INDEX_C_CREDIT_LIM = 7;
    private static int INDEX_C_DISCOUNT = 8;
    private static int INDEX_C_FULL_BALANCE = 9;

    private static int INDEX_WAREHOUSE = 0;
    private static int INDEX_DISTRICT = 1;

    @Override
    public void executeFlow() {
        List<Object> args = Lists.newArrayList(data.getC_W_ID(), data.getC_D_ID(), data.getC_ID());
        QueryExecutor.getInstance().getAndUpdateWithRetry(PStatement.GET_BALANCE_PAYMENT, args, PStatement.UPDATE_BALANCE_PAYMENT, args,
                row -> {
                    List<Object> values = Lists.newArrayList();
                    values.add(row.getDouble(INDEX_C_BALANCE) - data.getPAYMENT());
                    values.add(row.getDouble(INDEX_C_YTD) + data.getPAYMENT());
                    values.add(row.getInt(INDEX_C_CNT) + 1);
                    return values;
                },
                row -> row.getInt(INDEX_C_CNT));

        Row customerRow = QueryExecutor.getInstance().executeAndGetOneRow(PStatement.GET_CUSTOMER_FULL, args);
        UDTValue customerAddress = customerRow.getUDTValue(INDEX_C_ADDRESS);
        Row wdRow = QueryExecutor.getInstance().executeAndGetOneRow(PStatement.GET_WD_ADDRESS, Lists.newArrayList(data.getC_W_ID(), data.getC_D_ID()));

        logger.info("Output information now!");

        System.out.println(String.format("1. (C_W_ID: %d, C_D_ID: %d, C_ID: %d), Name: (%s, %s, %s), Address: (%s, %s, %s, %s, %s), C_PHONE: %s, C_SINCE: %s, C_CREDIT: %s, C_CREDIT_LIM: %.4f, C_DISCOUNT: %.4f, C_BALANCE: %.4f",
                data.getC_W_ID(), data.getC_D_ID(), data.getC_ID(),
                customerRow.getString(INDEX_C_FIRST), customerRow.getString(INDEX_C_MIDDLE), customerRow.getString(INDEX_C_LAST),
                customerAddress.getString("street_1"), customerAddress.getString("street_2"), customerAddress.getString("city"), customerAddress.getString("state"), customerAddress.getString("zip"),
                customerRow.getString(INDEX_C_PHONE), TimeUtility.format(customerRow.getTimestamp(INDEX_C_SINCE)), customerRow.getString(INDEX_C_CREDIT), customerRow.getDouble(INDEX_C_CREDIT_LIM), customerRow.getDouble(INDEX_C_DISCOUNT), customerRow.getDouble(INDEX_C_FULL_BALANCE)
        ));

        UDTValue warehouseAddress = wdRow.getUDTValue(INDEX_WAREHOUSE);
        UDTValue districtAddress = wdRow.getUDTValue(INDEX_DISTRICT);

        System.out.println(String.format("2. Warehouse: %s, %s, %s, %s, %s",
                warehouseAddress.getString("street_1"), warehouseAddress.getString("street_2"), warehouseAddress.getString("city"), warehouseAddress.getString("state"), warehouseAddress.getString("zip")));
        System.out.println(String.format("3. District: %s, %s, %s, %s, %s",
                districtAddress.getString("street_1"), districtAddress.getString("street_2"), districtAddress.getString("city"), districtAddress.getString("state"), districtAddress.getString("zip")));
    }
}
