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
        for (short O_D_ID = 1; O_D_ID < 11; O_D_ID++) {
            Row minUndeliveredOrderIdRow = QueryExecutor.getInstance().getAndUpdateWithRetry(PStatement.GET_MIN_ORDER_ID_WITH_NULL_CARRIER_ID, Lists.newArrayList(data.getW_ID(), O_D_ID),
                    PStatement.UPDATE_MIN_ORDER_ID_WITH_NULL_CARRIER_ID,
                    Lists.newArrayList(data.getW_ID(), O_D_ID),
                    row ->Collections.singletonList(row.getInt(INDEX_MIN_UNDELIVERED_ORDER_ID)+1),
                    row -> row.getInt(INDEX_MIN_UNDELIVERED_ORDER_ID));
            int minUndeliveredOrderId = minUndeliveredOrderIdRow.getInt(INDEX_MIN_UNDELIVERED_ORDER_ID);
            int customerId = QueryExecutor.getInstance().executeAndGetOneRow(PStatement.GET_CUSTOMER_ID_FROM_ORDER, Lists.newArrayList(data.getW_ID(), O_D_ID, minUndeliveredOrderId)).getInt(INDEX_CUSTOMER_ID);
            ResultSet orderLines = QueryExecutor.getInstance().execute(PStatement.GET_ORDER_LINES, Lists.newArrayList(data.getW_ID(), O_D_ID, minUndeliveredOrderId));
            int orderLineAmount = 0;
            for(Row orderLine : orderLines) {
                orderLineAmount += orderLine.getInt(INDEX_ORDER_LINE_AMOUNT);
                int orderLineNumber = orderLine.getInt(INDEX_ORDER_LINE_NUMBER);
                QueryExecutor.getInstance().execute(PStatement.UPDATE_ORDER_LINES_DELIVERY_DATE, Lists.newArrayList(new Date(), data.getW_ID(), O_D_ID, minUndeliveredOrderId, orderLineNumber));
            }
            int totalOrderLineAmount = orderLineAmount;
            QueryExecutor.getInstance().getAndUpdateWithRetry(PStatement.GET_CUSTOMER_BALANCE_AND_DELIVERY_COUNT, Lists.newArrayList(data.getW_ID(), O_D_ID, customerId),
                    PStatement.UPDATE_CUSTOMER_BALANCE_AND_DELIVERY_COUNT,
                    Lists.newArrayList(data.getW_ID(), O_D_ID, customerId),
                    row -> {
                        List<Object> values = Lists.newArrayList();
                            if (row.getDouble(INDEX_CUSTOMER_BALANCE) != (Double) null) {
                                values.add(row.getDouble(INDEX_CUSTOMER_BALANCE) + totalOrderLineAmount);
                            } else {
                                values.add(null);
                            }
                            if (row.getInt(INDEX_CUSTOMER_DELIVERY_COUNT) != (Integer) null) {
                                values.add(row.getInt(INDEX_CUSTOMER_DELIVERY_COUNT) + 1);
                            } else {
                                values.add(null);
                            }
                        return values;
                    },
                    row -> row == null || row.getInt(INDEX_CUSTOMER_DELIVERY_COUNT) == (Integer) null ? null : row.getInt(INDEX_CUSTOMER_DELIVERY_COUNT));
        }
    }
}
