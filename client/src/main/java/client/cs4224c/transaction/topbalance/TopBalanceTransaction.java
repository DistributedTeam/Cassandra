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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class TopBalanceTransaction extends AbstractTransaction {

    private final Logger logger = LoggerFactory.getLogger(TopBalanceTransaction.class);

    private static int INDEX_W_ID = 0;
    private static int INDEX_D_ID = 1;
    private static int INDEX_W_NAME = 2;
    private static int INDEX_D_NAME = 3;

    private static int INDEX_C_FIRST = 0;
    private static int INDEX_C_MIDDLE = 1;
    private static int INDEX_C_LAST = 2;
    private static int INDEX_C_BALANCE = 3;

    @Override
    public void executeFlow() {   
        ResultSet warehouseDistrictRows = QueryExecutor.getInstance().execute(PStatement.GET_W_ID_D_ID_W_NAME_D_NAME, Lists.newArrayList());
        List<CustomerData> customerList = new ArrayList<CustomerData>();
        
        for(Row warehouseDistrict : warehouseDistrictRows) {
            short W_ID = warehouseDistrict.getShort(INDEX_W_ID);
            short D_ID = warehouseDistrict.getShort(INDEX_D_ID);
            String W_NAME = warehouseDistrict.getString(INDEX_W_NAME);
            String D_NAME = warehouseDistrict.getString(INDEX_D_NAME);
            List<Object> args = Lists.newArrayList(W_ID, D_ID);
            ResultSet customerRows = QueryExecutor.getInstance().execute(PStatement.GET_TOP_BALANCE_CUSTOMER, args);
            for(Row customer : customerRows) {
                String C_FIRST = customer.getString(INDEX_C_FIRST);
                String C_MIDDLE = customer.getString(INDEX_C_MIDDLE);
                String C_LAST = customer.getString(INDEX_C_LAST);
                double C_BALANCE = customer.getDouble(INDEX_C_BALANCE);
                customerList.add(new CustomerData(W_NAME, D_NAME, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE));
            }
        }

        // Descending order
        Collections.sort(customerList, new Comparator<CustomerData>(){
            public int compare(CustomerData c1, CustomerData c2){
                double balance1 = c1.getC_BALANCE();
                double balance2 = c2.getC_BALANCE();
                if(balance1 == balance2)
                    return 0;
                return balance1 < balance2 ? 1 : -1;
            }
       });
        
        logger.info("Output information");
        
        for (int i = 0; i < 10; i++) {
            CustomerData customer = customerList.get(i);
            System.out.println(String. format("%d. Name: (%s, %s, %s), Balance: %.4f, Warehouse Name: %s, District Name: %s",
                    i + 1, customer.getC_FIRST(), customer.getC_MIDDLE(), customer.getC_LAST(),
                    customer.getC_BALANCE(), customer.getW_NAME(), customer.getD_NAME()));
        }
    }
}
