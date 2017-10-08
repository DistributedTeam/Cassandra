package client.cs4224c.transaction.payment;

import client.cs4224c.transaction.AbstractTransaction;
import client.cs4224c.transaction.payment.data.PaymentTransactionData;
import client.cs4224c.util.PStatement;
import client.cs4224c.util.QueryExecutor;
import client.cs4224c.util.TimeUtility;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.UDTValue;
import com.google.common.collect.Lists;
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
        Row balanceRow = QueryExecutor.getInstance().executeAndGetOneRow(PStatement.GET_BALANCE, args);
        double C_BALANCE = (balanceRow.getLong(INDEX_C_BALANCE) - (long)(data.getPAYMENT() * 100)) / 100.0; // DECIMAL(12,2)
        logger.info("Update payment for customer and warehouse_district.");
        QueryExecutor.getInstance().execute(PStatement.UPDATE_BALANCE_PAYMENT, Lists.newArrayList(-(long)(data.getPAYMENT() * 100), (long)(data.getPAYMENT() * 10000), 1L,
                data.getC_W_ID(), data.getC_D_ID(), data.getC_ID())); // DECIMAL(12,2) DECIMAL(12,4)
        QueryExecutor.getInstance().execute(PStatement.UPDATE_BALANCE_PARTIAL, Lists.newArrayList(C_BALANCE, data.getC_W_ID(), data.getC_D_ID(), data.getC_ID()));
        QueryExecutor.getInstance().execute(PStatement.UPDATE_WAREHOUSE_PAYMENT, Lists.newArrayList((long)(data.getPAYMENT() * 100), data.getC_W_ID(), data.getC_D_ID()));
        // DECIMAL(12,2)

        Row customerRow = QueryExecutor.getInstance().executeAndGetOneRow(PStatement.GET_CUSTOMER_FULL, args);
        UDTValue customerAddress = customerRow.getUDTValue(INDEX_C_ADDRESS);
        Row wdRow = QueryExecutor.getInstance().executeAndGetOneRow(PStatement.GET_WD_ADDRESS, Lists.newArrayList(data.getC_W_ID(), data.getC_D_ID()));

        logger.info("Output information now!");

        System.out.println(String.format("1. (C_W_ID: %d, C_D_ID: %d, C_ID: %d), Name: (%s, %s, %s), Address: (%s, %s, %s, %s, %s), C_PHONE: %s, C_SINCE: %s, C_CREDIT: %s, C_CREDIT_LIM: %.2f, C_DISCOUNT: %.4f, C_BALANCE: %.2f",
                data.getC_W_ID(), data.getC_D_ID(), data.getC_ID(),
                customerRow.getString(INDEX_C_FIRST), customerRow.getString(INDEX_C_MIDDLE), customerRow.getString(INDEX_C_LAST),
                customerAddress.getString("street_1"), customerAddress.getString("street_2"), customerAddress.getString("city"), customerAddress.getString("state"), customerAddress.getString("zip"),
                customerRow.getString(INDEX_C_PHONE), TimeUtility.format(customerRow.getTimestamp(INDEX_C_SINCE)), customerRow.getString(INDEX_C_CREDIT), customerRow.getDouble(INDEX_C_CREDIT_LIM), customerRow.getDouble(INDEX_C_DISCOUNT),
                C_BALANCE // DECIMAL(12,2)
        ));

        UDTValue warehouseAddress = wdRow.getUDTValue(INDEX_WAREHOUSE);
        UDTValue districtAddress = wdRow.getUDTValue(INDEX_DISTRICT);

        System.out.println(String.format("2. Warehouse: %s, %s, %s, %s, %s",
                warehouseAddress.getString("street_1"), warehouseAddress.getString("street_2"), warehouseAddress.getString("city"), warehouseAddress.getString("state"), warehouseAddress.getString("zip")));
        System.out.println(String.format("3. District: %s, %s, %s, %s, %s",
                districtAddress.getString("street_1"), districtAddress.getString("street_2"), districtAddress.getString("city"), districtAddress.getString("state"), districtAddress.getString("zip")));
        System.out.println(String.format("4. PAYMENT: %.2f", data.getPAYMENT()));
    }
}
