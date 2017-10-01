package client.cs4224c.transaction.neworder;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.collect.Lists;
import client.cs4224c.transaction.AbstractTransaction;
import client.cs4224c.transaction.neworder.data.NewOrderTransactionData;
import client.cs4224c.transaction.neworder.data.NewOrderTransactionOrderLine;
import client.cs4224c.util.PStatement;
import client.cs4224c.util.QueryExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Date;
import java.util.List;

public class NewOrderTransaction extends AbstractTransaction {

    private final Logger logger = LoggerFactory.getLogger(NewOrderTransaction.class);

    private NewOrderTransactionData data;

    public NewOrderTransactionData getData() {
        return data;
    }

    public void setData(NewOrderTransactionData data) {
        this.data = data;
    }

    private static int INDEX_FIRST = 0;

    private static int INDEX_NEXT_O_ID = 0;
    private static int INDEX_W_TAX = 1;
    private static int INDEX_D_TAX = 2;

    private static int INDEX_STOCK_QUANTITY = 0;
    private static int INDEX_STOCK_YTD = 1;
    private static int INDEX_STOCK_ORDER_CNT = 2;
    private static int INDEX_STOCK_REMOTE_CNT = 3;
    private static int INDEX_STOCK_ITEM_PRICE = 0;
    private static int INDEX_STOCK_ITEM_NAME = 1;
    private static int INDEX_STOCK_DIST_INFO = 2;

    private static int INDEX_CUSTOMER_DISCOUNT = 0;
    private static int INDEX_CUSTOMER_LAST = 1;
    private static int INDEX_CUSTOMER_CREDIT = 2;

    @Override
    public void executeFlow() {
        logger.info("Get and update d_next_o_id");
        List<Object> args = Lists.newArrayList(data.getW_ID(), data.getD_ID());
        Row warehouseDistrictRow = QueryExecutor.getInstance().getAndUpdateWithRetry(PStatement.GET_D_NEXT_O_ID, args, PStatement.UPDATE_D_NEXT_O_ID, args,
                row -> Collections.singletonList(row.getInt(INDEX_NEXT_O_ID) + 1), row -> row.getInt(INDEX_NEXT_O_ID));
        int next_o_id = warehouseDistrictRow.getInt(INDEX_NEXT_O_ID);
        data.setW_TAX(warehouseDistrictRow.getDouble(INDEX_W_TAX));
        data.setD_TAX(warehouseDistrictRow.getDouble(INDEX_D_TAX));
        logger.info("Get the D_NEXT_O_ID and update it with retry already {}", next_o_id);

        logger.info("Create new order");
        ResultSet resultSet = QueryExecutor.getInstance().execute(PStatement.INSERT_ORDER,
                Lists.newArrayList(next_o_id, data.getD_ID(), data.getW_ID(), data.getC_ID(), new Date(), data.getOrderLines().size(), data.isAllLocal()));
        if (!resultSet.wasApplied()) {
            throw new RuntimeException("Cannot insert order, the query is not applied");
        }

        double total_amount = 0;
        for (int i = 0; i < data.getOrderLines().size(); i++) {
            logger.info("Create order-line[{}]", i);
            NewOrderTransactionOrderLine orderLine = data.getOrderLines().get(i);
            List<Object> orderLineArgs = Lists.newArrayList(orderLine.getOL_SUPPLY_W_ID(), orderLine.getOL_I_ID());

            QueryExecutor.getInstance().getAndUpdateWithRetry(PStatement.GET_STOCK, orderLineArgs, PStatement.UPDATE_STOCK, orderLineArgs,
                    row -> {
                        int quantity = row.getInt(INDEX_STOCK_QUANTITY) - orderLine.getOL_QUANTITY();
                        if (quantity < 10) {
                            quantity += 100;
                        }
                        orderLine.setS_QUANTITY(quantity); // updated quantity for the item
                        int remoteCnt = row.getInt(INDEX_STOCK_REMOTE_CNT);
                        if (orderLine.getOL_SUPPLY_W_ID() != data.getW_ID()) {
                            remoteCnt++;
                        }
                        return Lists.newArrayList(quantity,
                                row.getDouble(INDEX_STOCK_YTD) + orderLine.getOL_QUANTITY(),
                                row.getInt(INDEX_STOCK_ORDER_CNT) + 1,
                                remoteCnt);
                    },
                    row -> row.getInt(INDEX_STOCK_ORDER_CNT));

            Row stockRow = QueryExecutor.getInstance().executeAndGetOneRow(PStatement.valueOf("GET_STOCK_DIST_" + data.getD_ID()), orderLineArgs);
            if (stockRow == null) {
                throw new RuntimeException("Empty stock row!");
            }

            double itemPrice = stockRow.getDouble(INDEX_STOCK_ITEM_PRICE);
            String distInfo = stockRow.getString(INDEX_STOCK_DIST_INFO);
            orderLine.setI_NAME(stockRow.getString(INDEX_STOCK_ITEM_NAME));

            double itemAmount = orderLine.getOL_QUANTITY() * itemPrice;
            orderLine.setOL_AMOUNT(itemAmount);
            total_amount += itemAmount;


            ResultSet olResultSet = QueryExecutor.getInstance().execute(PStatement.INSERT_ORDER_LINE, Lists.newArrayList(data.getW_ID(), data.getD_ID(), next_o_id, i + 1, orderLine.getOL_I_ID(),
                    orderLine.getOL_SUPPLY_W_ID(), orderLine.getOL_QUANTITY(), itemAmount, distInfo, orderLine.getI_NAME()));
            if (!olResultSet.wasApplied()) {
                throw new RuntimeException("Cannot insert order-line, the query is not applied");
            }
        }

        Row customerRow = QueryExecutor.getInstance().executeAndGetOneRow(PStatement.GET_CUSTOMER, Lists.newArrayList(data.getW_ID(), data.getD_ID(), data.getC_ID()));
        if (customerRow == null) {
            throw new RuntimeException("Empty costumer in database.");
        }
        data.setC_DISCOUNT(customerRow.getDouble(INDEX_CUSTOMER_DISCOUNT));
        data.setC_CREDIT(customerRow.getString(INDEX_CUSTOMER_CREDIT));
        data.setC_LAST(customerRow.getString(INDEX_CUSTOMER_LAST));

        total_amount *= (1 + data.getD_TAX() + data.getW_TAX()) * (1 - data.getC_DISCOUNT());

        logger.error("Output information now!");
        System.out.println(String.format("1. (W_ID: %d, D_ID: %d, C_ID, %d), C_LAST: %s, C_CREDIT: %s, C_DISCOUNT: %.4f", data.getW_ID(), data.getD_ID(), data.getC_ID(),
                data.getC_LAST(), data.getC_CREDIT(), data.getC_DISCOUNT()));
        System.out.println(String.format("2. W_TAX: %.4f, D_TAX: %.4f", data.getW_TAX(), data.getD_TAX()));
        System.out.println(String.format("3. NUM_ITEMS: %s, TOTAL_AMOUNT: %.4f", data.getOrderLines().size(), total_amount));
        System.out.println("4. DETAILS OF ITEMS");
        for (NewOrderTransactionOrderLine orderLine : data.getOrderLines()) {
            System.out.println(String.format("\t ITEM_NUMBER: %s, I_NAME: %s, SUPPLIER_WAREHOUSE: %d, QUANTITY: %d, OL_AMOUNT: %.4f, S_QUANTITY: %d",
                    orderLine.getOL_I_ID(), orderLine.getI_NAME(), orderLine.getOL_SUPPLY_W_ID(), orderLine.getOL_QUANTITY(), orderLine.getOL_AMOUNT(), orderLine.getS_QUANTITY()));
        }
    }
}
