package client.cs4224c.transaction.stocklevel;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.UDTValue;
import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import client.cs4224c.transaction.AbstractTransaction;
import client.cs4224c.transaction.stocklevel.data.StockLevelTransactionData;
import client.cs4224c.util.PStatement;
import client.cs4224c.util.QueryExecutor;
import client.cs4224c.util.TimeUtility;

public class StockLevelTransaction extends AbstractTransaction {

    private final Logger logger = LoggerFactory.getLogger(StockLevelTransaction.class);

    private StockLevelTransactionData data;

    public StockLevelTransactionData getData() {
        return data;
    }

    public void setData(StockLevelTransactionData data) {
        this.data = data;
    }

    private static int INDEX_W_ID = 0;
    private static int INDEX_D_ID = 1;
    private static int INDEX_T = 2;
    private static int INDEX_L = 3;

    private static int INDEX_NEXT_O_ID = 0;

    private static int INDEX_I_ID = 0;

    private static int INDEX_QUANTITY = 0;

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

            ResultSet orderLineItems = QueryExecutor.getInstance().execute(PStatement.GET_LAST_L_ORDER_ITEMS, Lists.newArrayList(data.getW_ID(), data.getD_ID(), i));
            if (orderLineItems == null) {
                throw new RuntimeException("Empty order line in database.");
            }
            for (Row orderLineItem:orderLineItems) {
                int i_id = orderLineItem.getInt(INDEX_I_ID);
                Row stockItemRow = QueryExecutor.getInstance().executeAndGetOneRow(PStatement.GET_STOCK_QUANTITY, Lists.newArrayList(data.getW_ID(), i_id));
                int quantity = stockItemRow.getInt(INDEX_QUANTITY);
                if (quantity < data.getT() && !data.getLowStockItems().contains(i_id)) // without duplicates
                    data.getLowStockItems().add(i_id);
            }
        }

        logger.info("Output information now!");

        System.out.println(String.format("%d", data.getLowStockItems().size()));
    }
}
