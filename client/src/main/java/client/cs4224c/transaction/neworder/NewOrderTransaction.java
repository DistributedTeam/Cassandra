package client.cs4224c.transaction.neworder;

import client.cs4224c.transaction.AbstractTransaction;
import client.cs4224c.transaction.neworder.data.NewOrderTransactionData;
import client.cs4224c.transaction.neworder.data.NewOrderTransactionOrderLine;
import client.cs4224c.util.PStatement;
import client.cs4224c.util.QueryExecutor;
import client.cs4224c.util.TimeUtility;
import com.datastax.driver.core.Row;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static int INDEX_W_TAX = 0;
    private static int INDEX_D_TAX = 1;

    private static int INDEX_STOCK_QUANTITY = 0;
    private static int INDEX_STOCK_YTD = 1;
    private static int INDEX_STOCK_ORDER_CNT = 2;
    private static int INDEX_STOCK_REMOTE_CNT = 3;
    private static int INDEX_STOCK_ITEM_PRICE = 0;
    private static int INDEX_STOCK_ITEM_NAME = 1;
    private static int INDEX_STOCK_DIST_INFO = 2;

    private static int INDEX_CUSTOMER_DISCOUNT = 0;
    private static int INDEX_CUSTOMER_FIRST = 1;
    private static int INDEX_CUSTOMER_MIDDLE = 2;
    private static int INDEX_CUSTOMER_LAST = 3;
    private static int INDEX_CUSTOMER_CREDIT = 4;

    @Override
    public void executeFlow() {
        logger.info("Get and update d_next_o_id");
        Row nextOIdRow = QueryExecutor.getInstance().executeAndGetOneRow(PStatement.GET_D_NEXT_O_ID, Lists.newArrayList(data.getW_ID(), data.getD_ID()));
        QueryExecutor.getInstance().execute(PStatement.UPDATE_D_NEXT_O_ID, Lists.newArrayList(1L, data.getW_ID(), data.getD_ID()));
        int next_o_id = (int)nextOIdRow.getLong(INDEX_NEXT_O_ID);

        Row warehouseDistrictRow = QueryExecutor.getInstance().executeAndGetOneRow(PStatement.GET_TAX, Lists.newArrayList(data.getW_ID(), data.getD_ID()));
        data.setW_TAX(warehouseDistrictRow.getDouble(INDEX_W_TAX));
        data.setD_TAX(warehouseDistrictRow.getDouble(INDEX_D_TAX));
        logger.info("Get the D_NEXT_O_ID and update it with retry already {}", next_o_id);

        Row customerRow = QueryExecutor.getInstance().executeAndGetOneRow(PStatement.GET_CUSTOMER, Lists.newArrayList(data.getW_ID(), data.getD_ID(), data.getC_ID()));
        data.setC_DISCOUNT(customerRow.getDouble(INDEX_CUSTOMER_DISCOUNT));
        data.setC_CREDIT(customerRow.getString(INDEX_CUSTOMER_CREDIT));
        data.setC_FIRST(customerRow.getString(INDEX_CUSTOMER_FIRST));
        data.setC_MIDDLE(customerRow.getString(INDEX_CUSTOMER_MIDDLE));
        data.setC_LAST(customerRow.getString(INDEX_CUSTOMER_LAST));

        logger.info("Create new order");
        data.setO_ENTRY_D(new Date());
        QueryExecutor.getInstance().execute(PStatement.INSERT_ORDER,
                Lists.newArrayList(next_o_id, data.getD_ID(), data.getW_ID(), data.getC_ID(), data.getO_ENTRY_D(), data.getOrderLines().size(), data.isAllLocal(),
                        data.getC_FIRST(), data.getC_MIDDLE(), data.getC_LAST()));

        logger.info("Update customer last order");
        QueryExecutor.getInstance().execute(PStatement.UPDATE_CUSTOMER_LAST_ORDER,
                Lists.newArrayList(next_o_id, data.getW_ID(), data.getD_ID(), data.getC_ID()));

        double total_amount = 0;
        for (int i = 0; i < data.getOrderLines().size(); i++) {
            logger.info("Create order-line[{}]", i);
            NewOrderTransactionOrderLine orderLine = data.getOrderLines().get(i);
            List<Object> orderLineArgs = Lists.newArrayList(orderLine.getOL_SUPPLY_W_ID(), orderLine.getOL_I_ID());

            Row stockQuantityRow = QueryExecutor.getInstance().execute(PStatement.GET_STOCK, orderLineArgs).one();
            long updatedQuantity = stockQuantityRow.getLong(INDEX_STOCK_QUANTITY) - orderLine.getOL_QUANTITY();
            long incrementQuantity = -orderLine.getOL_QUANTITY();
            long incrementRemoteCnt = 0;
            long incrementOrderCnt = 1;
            long incrementStockYtd = orderLine.getOL_QUANTITY() * 100; // DECIMAL(8,2)
            if (updatedQuantity < 10) {
                incrementQuantity += 100;
            }
            if (orderLine.getOL_SUPPLY_W_ID() != data.getW_ID()) {
                incrementRemoteCnt++;
            }
            logger.info("Update stock");
            QueryExecutor.getInstance().execute(PStatement.UPDATE_STOCK, Lists.newArrayList(incrementQuantity, incrementStockYtd, incrementOrderCnt, incrementRemoteCnt,
                    orderLine.getOL_SUPPLY_W_ID(), orderLine.getOL_I_ID()));
            orderLine.setS_QUANTITY((int)(stockQuantityRow.getLong(INDEX_STOCK_QUANTITY) + incrementQuantity));

            Row stockRow = QueryExecutor.getInstance().executeAndGetOneRow(PStatement.valueOf("GET_STOCK_DIST_" + data.getD_ID()), orderLineArgs);

            double itemPrice = stockRow.getDouble(INDEX_STOCK_ITEM_PRICE);
            String distInfo = stockRow.getString(INDEX_STOCK_DIST_INFO);
            orderLine.setI_NAME(stockRow.getString(INDEX_STOCK_ITEM_NAME));

            double itemAmount = orderLine.getOL_QUANTITY() * itemPrice;
            orderLine.setOL_AMOUNT(itemAmount);
            total_amount += itemAmount;


            QueryExecutor.getInstance().execute(PStatement.INSERT_ORDER_LINE, Lists.newArrayList(data.getW_ID(), data.getD_ID(), next_o_id, i + 1, orderLine.getOL_I_ID(),
                    orderLine.getOL_SUPPLY_W_ID(), orderLine.getOL_QUANTITY(), itemAmount, distInfo, orderLine.getI_NAME()));
        }

        total_amount *= (1 + data.getD_TAX() + data.getW_TAX()) * (1 - data.getC_DISCOUNT());

        logger.info("Output information now!");
        System.out.println(String.format("1. (W_ID: %d, D_ID: %d, C_ID, %d), C_LAST: %s, C_CREDIT: %s, C_DISCOUNT: %.4f", data.getW_ID(), data.getD_ID(), data.getC_ID(),
                data.getC_LAST(), data.getC_CREDIT(), data.getC_DISCOUNT()));
        System.out.println(String.format("2. W_TAX: %.4f, D_TAX: %.4f", data.getW_TAX(), data.getD_TAX()));
        System.out.println(String.format("3. O_ID: %d, O_ENTRY_D: %s", next_o_id, TimeUtility.format(data.getO_ENTRY_D())));
        System.out.println(String.format("4. NUM_ITEMS: %s, TOTAL_AMOUNT: %.2f", data.getOrderLines().size(), total_amount));
        System.out.println("5. DETAILS OF ITEMS");
        for (NewOrderTransactionOrderLine orderLine : data.getOrderLines()) {
            System.out.println(String.format("\t ITEM_NUMBER: %s, I_NAME: %s, SUPPLIER_WAREHOUSE: %d, QUANTITY: %d, OL_AMOUNT: %.2f, S_QUANTITY: %d",
                    orderLine.getOL_I_ID(), orderLine.getI_NAME(), orderLine.getOL_SUPPLY_W_ID(), orderLine.getOL_QUANTITY(), orderLine.getOL_AMOUNT(), orderLine.getS_QUANTITY()));
        }
    }
}
