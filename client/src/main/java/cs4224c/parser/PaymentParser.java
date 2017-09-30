package cs4224c.parser;

import cs4224c.transaction.AbstractTransaction;
import cs4224c.transaction.neworder.NewOrderTransaction;
import cs4224c.transaction.neworder.data.NewOrderTransactionData;
import cs4224c.transaction.neworder.data.NewOrderTransactionOrderLine;
import cs4224c.transaction.payment.PaymentTransaction;
import cs4224c.transaction.payment.data.PaymentTransactionData;
import cs4224c.util.Constant;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class PaymentParser extends AbstractParser {

    private static int INDEX_C_W_ID = 1;
    private static int INDEX_C_D_ID = 2;
    private static int INDEX_C_ID = 3;
    private static int INDEX_PAYMENT = 4;

    @Override
    public AbstractTransaction parse(Scanner sc, String[] arguments) {
        PaymentTransaction transaction = new PaymentTransaction();

        PaymentTransactionData data = new PaymentTransactionData();
        data.setC_W_ID(Short.parseShort(arguments[INDEX_C_W_ID]));
        data.setC_D_ID(Short.parseShort(arguments[INDEX_C_D_ID]));
        data.setC_ID(Integer.parseInt(arguments[INDEX_C_ID]));
        data.setPAYMENT(Double.parseDouble(arguments[INDEX_PAYMENT]));

        transaction.setData(data);

        return transaction;
    }

}
