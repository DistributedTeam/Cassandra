package client.cs4224c.transaction.stocklevel;

import client.cs4224c.transaction.AbstractTransaction;
import client.cs4224c.transaction.stocklevel.data.StockLevelTransactionData;
import client.cs4224c.util.PStatement;
import client.cs4224c.util.QueryExecutor;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StockLevelTransaction extends AbstractTransaction {

    private final Logger logger = LoggerFactory.getLogger(StockLevelTransaction.class);

    private StockLevelTransactionData data;

    public StockLevelTransactionData getData() {
        return data;
    }

    public void setData(StockLevelTransactionData data) {
        this.data = data;
    }

    private static int INDEX_NEXT_O_ID = 0;

    private static int INDEX_I_ID = 0;

    private static int INDEX_QUANTITY = 0;

    @Override
    public void executeFlow() {

        Row warehouseDistrictRow = QueryExecutor.getInstance().executeAndGetOneRow(PStatement.GET_NEXT_ORDER_NUMBER, Lists.newArrayList(data.getW_ID(), data.getD_ID()));
        if (warehouseDistrictRow == null) {
            throw new RuntimeException("Empty district in database.");
        }
        int next_o_id = (int)warehouseDistrictRow.getLong(INDEX_NEXT_O_ID);

        for (int i = next_o_id - data.getL(); i < next_o_id; i++) {

            // validation of L
            if (i <= 0) {
                logger.warn("No more orders for warehouse {} district {} already.", data.getW_ID(), data.getD_ID());
                continue;
            }

            ResultSet orderLineItems = QueryExecutor.getInstance().execute(PStatement.GET_LAST_L_ORDER_ITEMS, Lists.newArrayList(data.getW_ID(), data.getD_ID(), i));
            if (!orderLineItems.iterator().hasNext()) {
                logger.warn("OrderLine cannot be find in database [{}, {}, {}]. This might due to interleave execution.", data.getW_ID(), data.getD_ID(), i);
                continue;
            }
            for (Row orderLineItem : orderLineItems) {
                int i_id = orderLineItem.getInt(INDEX_I_ID);

                if (data.getOrderlineItems().contains(i_id)) {
                    // without duplicates
                    // no need to query as it is already there.
                    // in case that someone minus stock in between this, we cannot do anything about it.
                    continue;
                }
                data.getOrderlineItems().add(i_id);

                Row stockItemRow = QueryExecutor.getInstance().executeAndGetOneRow(PStatement.GET_STOCK_QUANTITY, Lists.newArrayList(data.getW_ID(), i_id));
                int quantity = (int)stockItemRow.getLong(INDEX_QUANTITY);
                if (quantity < data.getT()) {
                    data.getLowStockItems().add(i_id);
                }
            }
        }

        logger.info("Output information now!");

        System.out.println(String.format("%d", data.getLowStockItems().size()));
    }
}
