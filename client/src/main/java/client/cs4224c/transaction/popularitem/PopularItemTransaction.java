package client.cs4224c.transaction.popularitem;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

import client.cs4224c.transaction.AbstractTransaction;
import client.cs4224c.transaction.popularitem.data.OrderData;
import client.cs4224c.transaction.popularitem.data.OrderLineItemData;
import client.cs4224c.transaction.popularitem.data.PopularItemTransactionData;
import client.cs4224c.util.PStatement;
import client.cs4224c.util.QueryExecutor;
import client.cs4224c.util.TimeUtility;

public class PopularItemTransaction extends AbstractTransaction {

    private final Logger logger = LoggerFactory.getLogger(PopularItemTransaction.class);

    private PopularItemTransactionData data;

    public PopularItemTransactionData getData() {
        return data;
    }

    public void setData(PopularItemTransactionData data) {
        this.data = data;
    }

    private static int INDEX_NEXT_O_ID = 0;

    private static int INDEX_ENTRY_DATE = 0;
    private static int INDEX_C_ID = 1;

    private static int INDEX_FIRST_NAME = 0;
    private static int INDEX_MIDDLE_NAME = 1;
    private static int INDEX_LAST_NAME = 2;

    private static int INDEX_I_ID = 0;
    private static int INDEX_QUANTITY = 1;
    private static int INDEX_ITEM_NAME = 2;

    private static int INDEX_FIRST = 0;

    @Override
    public void executeFlow() {

        Row warehouseDistrictRow = QueryExecutor.getInstance().executeAndGetOneRow(PStatement.GET_NEXT_ORDER_NUMBER, Lists.newArrayList(data.getW_ID(), data.getD_ID()));
        if (warehouseDistrictRow == null) {
            throw new RuntimeException("Empty district in database.");
        }
        int next_o_id = warehouseDistrictRow.getInt(INDEX_NEXT_O_ID);

        for (int i = next_o_id - data.getL(); i < next_o_id; i++) {

            // validation of L
            if (i <= 0)
                continue;

            Row orderRow = QueryExecutor.getInstance().executeAndGetOneRow(PStatement.GET_LAST_L_ORDERS, Lists.newArrayList(data.getW_ID(), data.getD_ID(), i));
            if (orderRow == null) {
                throw new RuntimeException("Empty order in database.");
            }

            int c_id = orderRow.getInt(INDEX_C_ID);
            Row customerRow = QueryExecutor.getInstance().executeAndGetOneRow(PStatement.GET_CUSTOMER_NAME, Lists.newArrayList(data.getW_ID(), data.getD_ID(), c_id));
            if (customerRow == null) {
                throw new RuntimeException("Empty costumer in database.");
            }

            OrderData orderData = new OrderData();
            orderData.setO_ID(i);
            orderData.setO_ENTRY_DATE(orderRow.getTimestamp(INDEX_ENTRY_DATE));
            orderData.setC_FIRST(customerRow.getString(INDEX_FIRST_NAME));
            orderData.setC_MIDDLE(customerRow.getString(INDEX_MIDDLE_NAME));
            orderData.setC_LAST(customerRow.getString(INDEX_LAST_NAME));

            ResultSet orderLines = QueryExecutor.getInstance().execute(PStatement.GET_LAST_L_ORDER_LINES, Lists.newArrayList(data.getW_ID(), data.getD_ID(), i));
            if (orderLines == null) {
                throw new RuntimeException("Empty order line in database.");
            }
            for (Row orderLine: orderLines) {
                OrderLineItemData orderLineItemData = new OrderLineItemData();
                orderLineItemData.setOL_I_ID(orderLine.getInt(INDEX_I_ID));
                orderLineItemData.setOL_QUANTITY(orderLine.getInt(INDEX_QUANTITY));
                orderLineItemData.setI_NAME(orderLine.getString(INDEX_ITEM_NAME));

                // Add to HashMap to keep track of orders containing order line item
                if (data.getItemsMap().get(orderLineItemData.getOL_I_ID()) == null) {
                    data.getItemsMap().put(orderLineItemData.getOL_I_ID(), new ArrayList<>());
                }
                if (!data.getItemsMap().get(orderLineItemData.getOL_I_ID()).contains(orderData.getO_ID()))
                    data.getItemsMap().get(orderLineItemData.getOL_I_ID()).add(orderData.getO_ID());

                // Find popular Item for each order
                if (orderData.getPopularItems().isEmpty() ||
                        orderLineItemData.getOL_QUANTITY() >= orderData.getPopularItems().get(INDEX_FIRST).getOL_QUANTITY()) {
                    if (!orderData.getPopularItems().isEmpty() &&
                            orderLineItemData.getOL_QUANTITY() > orderData.getPopularItems().get(INDEX_FIRST).getOL_QUANTITY())
                        orderData.getPopularItems().clear();
                    orderData.getPopularItems().add(orderLineItemData);
                }
            }

            data.getLastLOrders().add(orderData);
        }

        logger.info("Output information now!");

        System.out.println(String.format("1. W_ID: %d, D_ID: %d", data.getW_ID(), data.getD_ID()));
        System.out.println(String.format("2. Number of last orders to be examined: %d", data.getL()));

        for (OrderData orderData: data.getLastLOrders()) {
            System.out.println(String.format("3.(a) O_ID: %d, O_ENTRY_D: %s", orderData.getO_ID(), TimeUtility.format(orderData.getO_ENTRY_DATE())));
            System.out.println(String.format("3.(b) C_FIRST: %s, C_MIDDLE: %s, C_LAST: %s)", orderData.getC_FIRST(), orderData.getC_MIDDLE(), orderData.getC_LAST()));

            System.out.println("3.(c)");
            for(OrderLineItemData popularItem: orderData.getPopularItems()) {
                System.out.println(String.format("(i) Item: %s", popularItem.getI_NAME()));
                System.out.println(String.format("(ii) Quantity Ordered: %d", popularItem.getOL_QUANTITY()));

                // Find distinct popular item
                if (!data.getPopularItems().contains(popularItem))
                    data.getPopularItems().add(popularItem);
            }
        }

        System.out.println("4.(a)");
        for (OrderLineItemData popularItem: data.getPopularItems())
        {
            System.out.println(String.format("(i) Item: %s", popularItem.getI_NAME()));
            System.out.println(String.format("(ii) Percentage of orders that contain the popular item: %.2f%", (data.getItemsMap().get(popularItem.getOL_I_ID()).size()/(data.getL()*1.0f))*100));
        }
    }
}
