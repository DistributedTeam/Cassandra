package transaction.neworder.data;

import java.util.List;

public class NewOrderTransactionData {

    private int C_ID;
    private int W_ID;
    private int D_ID;
    private int M;

    private List<NewOrderTransactionOrderLine> orderLines;

    public int getC_ID() {
        return C_ID;
    }

    public void setC_ID(int c_ID) {
        C_ID = c_ID;
    }

    public int getW_ID() {
        return W_ID;
    }

    public void setW_ID(int w_ID) {
        W_ID = w_ID;
    }

    public int getD_ID() {
        return D_ID;
    }

    public void setD_ID(int d_ID) {
        D_ID = d_ID;
    }

    public int getM() {
        return M;
    }

    public void setM(int m) {
        M = m;
    }

    public List<NewOrderTransactionOrderLine> getOrderLines() {
        return orderLines;
    }

    public void setOrderLines(List<NewOrderTransactionOrderLine> orderLines) {
        this.orderLines = orderLines;
    }
}
