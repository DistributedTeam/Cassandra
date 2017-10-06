package client.cs4224c.transaction.orderstatus;

import client.cs4224c.transaction.AbstractTransaction;
import client.cs4224c.transaction.orderstatus.data.OrderStatusTransactionData;
import client.cs4224c.util.PStatement;
import client.cs4224c.util.QueryExecutor;
import client.cs4224c.util.TimeUtility;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class OrderStatusTransaction extends AbstractTransaction {
    private final Logger logger = LoggerFactory.getLogger(OrderStatusTransaction.class);

    private OrderStatusTransactionData data;

    public OrderStatusTransactionData getData() {
        return data;
    }

    public void setData(OrderStatusTransactionData data) {
        this.data = data;
    }

    private static int INDEX_ORDER_ENTRY_DATE = 0;
    private static int INDEX_ORDER_CARRIER_ID = 1;

    private static int INDEX_ORDER_LINE_ID = 0;
    private static int INDEX_ORDER_LINE_SUPPLY_WAREHOUSE_ID = 1;
    private static int INDEX_ORDER_LINE_QUANTITY = 2;
    private static int INDEX_ORDER_LINE_AMOUNT = 3;
    private static int INDEX_ORDER_LINE_DELIVERY_DATE = 4;

    private static int INDEX_CUSTOMER_FIRSR_NAME = 0;
    private static int INDEX_CUSTOMER_MIDDLE_NAME = 1;
    private static int INDEX_CUSTOMER_LAST_NAME = 2;
    private static int INDEX_CUSTOMER_BALANCE = 3;
    private static int INDEX_CUSTOMER_LAST_ORDER_ID = 4;

    @Override
    public void executeFlow() {
        Row customerRow = QueryExecutor.getInstance().executeAndGetOneRow(PStatement.GET_CUSTOMER_NAME_AND_BALANCE, Lists.newArrayList(data.getC_W_ID(), data.getC_D_ID(), data.getC_ID()));
        if (customerRow == null) {
            throw new RuntimeException(String.format("Customer C_W_ID = %d, C_D_ID = %d, C_ID = %s doesn't exist!", data.getC_W_ID(), data.getC_D_ID(), data.getC_ID()));
        }

        System.out.println(String.format("1. Customer's name (C_FIRST: %s, C_MIDDLE: %s, C_LAST: %s), balance C_BALANCE: %.4f",
                customerRow.getString(INDEX_CUSTOMER_FIRSR_NAME),
                customerRow.getString(INDEX_CUSTOMER_MIDDLE_NAME),
                customerRow.getString(INDEX_CUSTOMER_LAST_NAME),
                customerRow.getDouble(INDEX_CUSTOMER_BALANCE)));

        Row lastOrderRow = QueryExecutor.getInstance().executeAndGetOneRow(PStatement.GET_CUSTOMER_LAST_ORDER, Lists.newArrayList(data.getC_W_ID(), data.getC_D_ID(), customerRow.getInt(INDEX_CUSTOMER_LAST_ORDER_ID)));
        if (lastOrderRow == null) {
            throw new RuntimeException(String.format("Order O_W_ID = %d, O_D_ID = %d, C_ID = %s doesn't exist!", data.getC_W_ID(), data.getC_D_ID(), customerRow.getInt(INDEX_CUSTOMER_LAST_ORDER_ID)));
        }

        System.out.println(String.format("2. Last order : order number O_ID: %s, entry date and time O_ENTRY_D: %s, carrier identifier O_CARRIER_ID: %s",
                customerRow.getInt(INDEX_CUSTOMER_LAST_ORDER_ID),
                TimeUtility.format(lastOrderRow.getTimestamp(INDEX_ORDER_ENTRY_DATE)),
                lastOrderRow.isNull(INDEX_ORDER_CARRIER_ID) ? "NO_CARRIER" : lastOrderRow.getInt(INDEX_ORDER_CARRIER_ID)));

        ResultSet orderLineRows = QueryExecutor.getInstance().execute(PStatement.GET_ORDER_LINES_FOR_LAST_ORDER, Lists.newArrayList(data.getC_W_ID(), data.getC_D_ID(), customerRow.getInt(INDEX_CUSTOMER_LAST_ORDER_ID)));

        System.out.println("3. Item in last order:");
        for (Row orderLine : orderLineRows) {
            Date OL_DELIVERY_D = orderLine.getTimestamp(INDEX_ORDER_LINE_DELIVERY_DATE);

            System.out.println(String.format("\tItem number OL_I_ID: %s, Supplying warehouse number OL_SUPPLY_W_ID: %d, Quantity ordered OL_QUANTITY: %d, Total price for ordered item OL_AMOUNT: %s, Data and time of delivery OL_DELIVERY_D: %s",
                    orderLine.getInt(INDEX_ORDER_LINE_ID),
                    orderLine.getShort(INDEX_ORDER_LINE_SUPPLY_WAREHOUSE_ID),
                    orderLine.getInt(INDEX_ORDER_LINE_QUANTITY),
                    orderLine.getDouble(INDEX_ORDER_LINE_AMOUNT),
                    OL_DELIVERY_D == null ? "NOT_DELIVERED" : TimeUtility.format(OL_DELIVERY_D)));
        }
    }
}
