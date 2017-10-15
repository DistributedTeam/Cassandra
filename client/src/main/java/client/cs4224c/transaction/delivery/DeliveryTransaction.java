package client.cs4224c.transaction.delivery;

import client.cs4224c.transaction.AbstractTransaction;
import client.cs4224c.transaction.delivery.data.DeliveryTransactionData;
import client.cs4224c.util.PStatement;
import client.cs4224c.util.QueryExecutor;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class DeliveryTransaction extends AbstractTransaction{

    private final Logger logger = LoggerFactory.getLogger(DeliveryTransaction.class);

    private DeliveryTransactionData data;

    public DeliveryTransactionData getData() {
        return data;
    }

    public void setData(DeliveryTransactionData data) {
        this.data = data;
    }

    private static int INDEX_MIN_UNDELIVERED_ORDER_ID = 0;

    private static int INDEX_CUSTOMER_ID = 0;

    private static int INDEX_ORDER_LINE_NUMBER = 0;
    private static int INDEX_ORDER_LINE_AMOUNT = 1;

    private static int INDEX_CUSTOMER_BALANCE = 0;

    @Override
    public void executeFlow() {
        for (short O_D_ID = 1; O_D_ID <= 10; O_D_ID++) {
            int minUndeliveredOrderId = 1; // if there is no row in DB, we assume the order number start from 1
            Row minUndeliveredOrderIdRow = QueryExecutor.getInstance().executeAndGetOneRow(PStatement.GET_MIN_ORDER_ID_WITH_NULL_CARRIER_ID, Lists.newArrayList(data.getW_ID(), O_D_ID));
            if (minUndeliveredOrderIdRow != null) {
                minUndeliveredOrderId = (int)minUndeliveredOrderIdRow.getLong(INDEX_MIN_UNDELIVERED_ORDER_ID);
            }

            ResultSet orderLines = QueryExecutor.getInstance().execute(PStatement.GET_ORDER_LINES, Lists.newArrayList(data.getW_ID(), O_D_ID, minUndeliveredOrderId));
            if (!orderLines.iterator().hasNext()) {
                logger.warn("No unDeliveredOrder for W_ID: {}, D_ID: {}, it doesn't make sense to make delivery transaction", data.getW_ID(), O_D_ID);
                continue;
            }
            // update next undelivered order
            QueryExecutor.getInstance().execute(PStatement.UPDATE_MIN_ORDER_ID_WITH_NULL_CARRIER_ID, Lists.newArrayList(1L, data.getW_ID(), O_D_ID));

            double orderLineAmount = 0;
            Date now = new Date();
            for(Row orderLine : orderLines) {
                orderLineAmount += orderLine.getDouble(INDEX_ORDER_LINE_AMOUNT);
                int orderLineNumber = orderLine.getInt(INDEX_ORDER_LINE_NUMBER);
                QueryExecutor.getInstance().execute(PStatement.UPDATE_ORDER_LINES_DELIVERY_DATE, Lists.newArrayList(now, data.getW_ID(), O_D_ID, minUndeliveredOrderId, orderLineNumber));
            }

            QueryExecutor.getInstance().execute(PStatement.UPDATE_ORDER_CARRIER_ID, Lists.newArrayList(data.getCARRIER_ID(), data.getW_ID(), O_D_ID, minUndeliveredOrderId));
            Row customerRow = QueryExecutor.getInstance().executeAndGetOneRow(PStatement.GET_CUSTOMER_ID_FROM_ORDER, Lists.newArrayList(data.getW_ID(), O_D_ID, minUndeliveredOrderId));
            int customerId = customerRow.getInt(INDEX_CUSTOMER_ID);

            double totalOrderLineAmount = orderLineAmount;
            QueryExecutor.getInstance().execute(PStatement.UPDATE_CUSTOMER_BALANCE_AND_DELIVERY_COUNT, Lists.newArrayList((long)(totalOrderLineAmount * 100), 1L,
                    data.getW_ID(), O_D_ID, customerId)); // DECIMAL(12,2)
            Row customerBalance = QueryExecutor.getInstance().executeAndGetOneRow(PStatement.GET_BALANCE, Lists.newArrayList(data.getW_ID(), O_D_ID, customerId));
            QueryExecutor.getInstance().execute(PStatement.UPDATE_BALANCE_PARTIAL, Lists.newArrayList(customerBalance.getLong(INDEX_CUSTOMER_BALANCE) / 100.0,
                    data.getW_ID(), O_D_ID, customerId)); // DECIMAL(12,2)
        }
    }
}
