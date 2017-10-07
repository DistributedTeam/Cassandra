package client.cs4224c.transaction.delivery;

import client.cs4224c.transaction.AbstractTransaction;
import client.cs4224c.transaction.delivery.data.DeliveryTransactionData;
import client.cs4224c.util.Constant;
import client.cs4224c.util.PStatement;
import client.cs4224c.util.QueryExecutor;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Date;
import java.util.List;

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
    private static int INDEX_CUSTOMER_DELIVERY_COUNT = 1;

    @Override
    public void executeFlow() {
        for (short O_D_ID = 1; O_D_ID <= 10; O_D_ID++) {
            Row minUndeliveredOrderIdRow = null;
            try {
                minUndeliveredOrderIdRow = QueryExecutor.getInstance().getAndUpdateWithRetry(PStatement.GET_MIN_ORDER_ID_WITH_NULL_CARRIER_ID, Lists.newArrayList(data.getW_ID(), O_D_ID),
                        PStatement.UPDATE_MIN_ORDER_ID_WITH_NULL_CARRIER_ID, Lists.newArrayList(data.getW_ID(), O_D_ID),
                        row -> {
                            if (row.getInt(INDEX_MIN_UNDELIVERED_ORDER_ID) == Constant.INVALID_O_ID) {
                                throw new IllegalArgumentException(); // no order for the district yet
                            }
                            return Collections.singletonList(row.getInt(INDEX_MIN_UNDELIVERED_ORDER_ID) + 1);
                        },
                        row -> row.getInt(INDEX_MIN_UNDELIVERED_ORDER_ID));
            } catch (IllegalArgumentException exception) {
                logger.warn("No order/unDeliveredOrder for W_ID: {}, D_ID: {}, it doesn't make sense to make delivery transaction", data.getW_ID(), O_D_ID);
                continue;
            }

            int minUndeliveredOrderId = minUndeliveredOrderIdRow.getInt(INDEX_MIN_UNDELIVERED_ORDER_ID);

            QueryExecutor.getInstance().execute(PStatement.UPDATE_ORDER_CARRIER_ID, Lists.newArrayList(data.getCARRIER_ID(), data.getW_ID(), O_D_ID, minUndeliveredOrderId));
            Row customerRow = QueryExecutor.getInstance().executeAndGetOneRow(PStatement.GET_CUSTOMER_ID_FROM_ORDER, Lists.newArrayList(data.getW_ID(), O_D_ID, minUndeliveredOrderId));
            if (customerRow.isNull(INDEX_CUSTOMER_ID)) {
                throw new RuntimeException(String.format("Empty customer from the order %d, %d, %d", data.getW_ID(), O_D_ID, minUndeliveredOrderId));
            }
            int customerId = customerRow.getInt(INDEX_CUSTOMER_ID);

            ResultSet orderLines = QueryExecutor.getInstance().execute(PStatement.GET_ORDER_LINES, Lists.newArrayList(data.getW_ID(), O_D_ID, minUndeliveredOrderId));
            double orderLineAmount = 0;
            for(Row orderLine : orderLines) {
                orderLineAmount += orderLine.getDouble(INDEX_ORDER_LINE_AMOUNT);
                int orderLineNumber = orderLine.getInt(INDEX_ORDER_LINE_NUMBER);
                QueryExecutor.getInstance().execute(PStatement.UPDATE_ORDER_LINES_DELIVERY_DATE, Lists.newArrayList(new Date(), data.getW_ID(), O_D_ID, minUndeliveredOrderId, orderLineNumber));
            }

            double totalOrderLineAmount = orderLineAmount;
            QueryExecutor.getInstance().getAndUpdateWithRetry(PStatement.GET_CUSTOMER_BALANCE_AND_DELIVERY_COUNT, Lists.newArrayList(data.getW_ID(), O_D_ID, customerId),
                    PStatement.UPDATE_CUSTOMER_BALANCE_AND_DELIVERY_COUNT,
                    Lists.newArrayList(data.getW_ID(), O_D_ID, customerId),
                    row -> {
                        List<Object> values = Lists.newArrayList();
                        values.add(row.getDouble(INDEX_CUSTOMER_BALANCE) + totalOrderLineAmount);
                        values.add(row.getInt(INDEX_CUSTOMER_DELIVERY_COUNT) + 1);
                        return values;
                    },
                    row -> row.getInt(INDEX_CUSTOMER_DELIVERY_COUNT));
        }
    }
}
