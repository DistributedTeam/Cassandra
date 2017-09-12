import com.google.common.collect.Lists;
import converter.*;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Massage {

    /**
     * Do a full massage for data file.
     */
    public static void main(String[] args) {
        ArrayList<AbstractConverter> abstractConverterArrayList = Lists.newArrayList(new Customer(), new Order(), new OrderLineItem(), new StockItem(), new WarehouseDistrict());

        ExecutorService executorService = Executors.newFixedThreadPool(6);

        for (AbstractConverter converter : abstractConverterArrayList) {
            try {
                executorService.execute(converter);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        executorService.shutdown();
        try {
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            System.out.println("Converter tasks may not be finished, encounter interrupted exception:" + e);
        }
    }
}
