package client.cs4224c.transaction.topbalance;

import client.cs4224c.transaction.AbstractTransaction;
import client.cs4224c.transaction.topbalance.data.CustomerData;
import client.cs4224c.util.PStatement;
import client.cs4224c.util.QueryExecutor;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class TopBalanceTransaction extends AbstractTransaction {

    private final Logger logger = LoggerFactory.getLogger(TopBalanceTransaction.class);

    private static int INDEX_W_ID = 0;
    private static int INDEX_D_ID = 1;

    private static int INDEX_C_BALANCE = 0;
    private static int INDEX_C_ID = 1;

    private static int INDEX_W_NAME = 0;
    private static int INDEX_D_NAME = 1;

    private static int INDEX_FIRST = 0;

    private static int INDEX_C_ROW_FIRST = 0;
    private static int INDEX_C_ROW_MIDDLE = 1;
    private static int INDEX_C_ROW_LAST = 2;

    @Override
    public void executeFlow() {   
        ResultSet warehouseDistrictRows = QueryExecutor.getInstance().execute(PStatement.GET_W_ID_D_ID, Lists.newArrayList());

        PriorityQueue<CustomerData> customers = new PriorityQueue<CustomerData>(new Comparator<CustomerData>(){
            public int compare(CustomerData c1, CustomerData c2){
                double balance1 = c1.getC_BALANCE();
                double balance2 = c2.getC_BALANCE();
                if(balance1 == balance2) {
                    return 0;
                }
                return balance1 < balance2 ? -1 : 1;
            }
        });

        for(Row warehouseDistrict : warehouseDistrictRows) {
            short W_ID = warehouseDistrict.getShort(INDEX_W_ID);
            short D_ID = warehouseDistrict.getShort(INDEX_D_ID);
            List<Object> args = Lists.newArrayList(W_ID, D_ID);
            ResultSet customerRows = QueryExecutor.getInstance().execute(PStatement.GET_TOP_BALANCE_CUSTOMER, args);
            for(Row customer : customerRows) {
                double C_BALANCE = customer.getDouble(INDEX_C_BALANCE);
                int C_ID = customer.getInt(INDEX_C_ID);
                customers.add(new CustomerData(W_ID, D_ID, C_ID, C_BALANCE));
                if (customers.size() >= 11) {
                    customers.poll();
                }
            }
        }

        logger.info("Output information");

        List<CustomerData> sortedCustomer = Lists.newArrayList(customers);
        // Descending order
        Collections.sort(sortedCustomer, new Comparator<CustomerData>(){
            public int compare(CustomerData c1, CustomerData c2){
                double balance1 = c1.getC_BALANCE();
                double balance2 = c2.getC_BALANCE();
                if(balance1 == balance2)
                    return 0;
                return balance1 < balance2 ? 1 : -1;
            }
        });
        Map<Short, String> wNameCache = new HashMap<Short, String>();
        Map<String, String> dNameCache = new HashMap<String, String>();
        
        for (int i = 0; i < 10; i++) {
            CustomerData customer = sortedCustomer.get(i);
            String tuple = String.join("-", String.valueOf(customer.getW_ID()), String.valueOf((customer.getD_ID())));
            if (!wNameCache.containsKey(customer.getW_ID()) && !dNameCache.containsKey(tuple)) {
                // fetch
                Row warehouseDistrictRow = QueryExecutor.getInstance().executeAndGetOneRow(PStatement.GET_W_D_NAME, Lists.newArrayList(customer.getW_ID(), customer.getD_ID()));
                wNameCache.put(customer.getW_ID(), warehouseDistrictRow.getString(INDEX_W_NAME));
                dNameCache.put(tuple, warehouseDistrictRow.getString(INDEX_D_NAME));
            }
            if (!wNameCache.containsKey(customer.getW_ID())) {
                Row warehouseRow = QueryExecutor.getInstance().executeAndGetOneRow(PStatement.GET_W_NAME, Lists.newArrayList(customer.getW_ID(), customer.getD_ID()));
                wNameCache.put(customer.getW_ID(), warehouseRow.getString(INDEX_W_NAME));
            }
            if (!dNameCache.containsKey(tuple)) {
                Row districtRow = QueryExecutor.getInstance().executeAndGetOneRow(PStatement.GET_D_NAME, Lists.newArrayList(customer.getW_ID(), customer.getD_ID()));
                dNameCache.put(tuple, districtRow.getString(INDEX_FIRST));
            }
            customer.setW_NAME(wNameCache.get(customer.getW_ID()));
            customer.setD_NAME(dNameCache.get(tuple));

            Row customerRow = QueryExecutor.getInstance().executeAndGetOneRow(PStatement.GET_CUSTOMER_NAME, Lists.newArrayList(customer.getW_ID(), customer.getD_ID(), customer.getC_ID()));
            customer.setC_FIRST(customerRow.getString(INDEX_C_ROW_FIRST));
            customer.setC_MIDDLE(customerRow.getString(INDEX_C_ROW_MIDDLE));
            customer.setC_LAST(customerRow.getString(INDEX_C_ROW_LAST));

            System.out.println(String. format("%d. Name: (%s, %s, %s), Balance: %.4f, Warehouse Name: %s, District Name: %s",
                    i + 1, customer.getC_FIRST(), customer.getC_MIDDLE(), customer.getC_LAST(),
                    customer.getC_BALANCE(), customer.getW_NAME(), customer.getD_NAME()));
        }
    }
}
