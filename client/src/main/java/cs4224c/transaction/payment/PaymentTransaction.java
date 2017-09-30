package cs4224c.transaction.payment;

import com.google.common.collect.Lists;
import cs4224c.transaction.AbstractTransaction;
import cs4224c.transaction.payment.data.PaymentTransactionData;
import cs4224c.util.PStatement;
import cs4224c.util.QueryExecutor;
import sun.plugin.cache.OldCacheEntry;

import java.util.Collections;
import java.util.List;

public class PaymentTransaction extends AbstractTransaction {

    private PaymentTransactionData data;

    public PaymentTransactionData getData() {
        return data;
    }

    public void setData(PaymentTransactionData data) {
        this.data = data;
    }

    private static int INDEX_C_CNT = 0;
    private static int INDEX_C_BALANCE = 1;
    private static int INDEX_C_YTD = 2;

    @Override
    public void executeFlow() {
        List<Object> args = Lists.newArrayList(data.getC_W_ID(), data.getC_D_ID(), data.getC_ID());
        QueryExecutor.getInstance().getAndUpdateWithRetry(PStatement.GET_BALANCE, args, PStatement.UPDATE_BALANCE_PAYMENT, args,
                row -> Lists.newArrayList(row.getInt(INDEX_C_CNT), row.getInt(INDEX_C_BALANCE), row.getDouble(INDEX_C_YTD)),
                oldValues -> {
                    List<Object> values = Lists.newArrayList(oldValues);
                    values.set(INDEX_C_CNT, (int) values.get(INDEX_C_CNT) + 1);
                    values.set(INDEX_C_CNT, (double) values.get(INDEX_C_BALANCE) + data.getPAYMENT());
                    values.set(INDEX_C_CNT, (double) values.get(INDEX_C_YTD) + data.getPAYMENT());
                    return values;
                });
    }
}
