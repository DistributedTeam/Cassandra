package parser;

import transaction.ITransaction;
import transaction.neworder.NewOrderTransaction;
import transaction.neworder.data.NewOrderTransactionData;
import transaction.neworder.data.NewOrderTransactionOrderLine;
import cs4224c.util.Constant;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class NewOrderParser implements IParser {

    private static int INDEX_C_ID = 1;
    private static int INDEX_W_ID = 2;
    private static int INDEX_D_ID = 3;
    private static int INDEX_M = 4;

    private static int INDEX_OL_I_ID = 0;
    private static int INDEX_OL_SUPPLY_W_ID = 1;
    private static int INDEX_OL_QUANTITY = 2;

    @Override
    public ITransaction parse(Scanner sc, String[] arguments) {
        NewOrderTransaction transaction = new NewOrderTransaction();

        NewOrderTransactionData data = new NewOrderTransactionData();
        data.setC_ID(Integer.parseInt(arguments[INDEX_C_ID]));
        data.setW_ID(Integer.parseInt(arguments[INDEX_W_ID]));
        data.setD_ID(Integer.parseInt(arguments[INDEX_D_ID]));
        data.setM(Integer.parseInt(arguments[INDEX_M]));

        List<NewOrderTransactionOrderLine> orderLines = new ArrayList<>();
        for (int i = 0; i < data.getM(); i++) {
            NewOrderTransactionOrderLine orderLine = new NewOrderTransactionOrderLine();
            String[] lineArguments = sc.nextLine().split(Constant.COMMA_SEPARATOR);

            orderLine.setOL_I_ID(Integer.parseInt(lineArguments[INDEX_OL_I_ID]));
            orderLine.setOL_SUPPLY_W_ID(Integer.parseInt(lineArguments[INDEX_OL_SUPPLY_W_ID]));
            orderLine.setOL_QUANTITY(Integer.parseInt(lineArguments[INDEX_OL_QUANTITY]));

            orderLines.add(orderLine);
        }
        data.setOrderLines(orderLines);

        transaction.setData(data);

        return transaction;
    }

}
