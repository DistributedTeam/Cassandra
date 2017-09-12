package transaction.neworder;

import transaction.ITransaction;
import transaction.neworder.data.NewOrderTransactionData;

public class NewOrderTransaction implements ITransaction {

    private NewOrderTransactionData data;

    public NewOrderTransactionData getData() {
        return data;
    }

    public void setData(NewOrderTransactionData data) {
        this.data = data;
    }
}
