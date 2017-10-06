package client.cs4224c.transaction.topbalance.data;

public class CustomerData {

    private String W_NAME;
    private String D_NAME;

    private double C_BALANCE;
    private String C_FIRST;
    private String C_MIDDLE;
    private String C_LAST;
    
    public CustomerData(String w_NAME, String d_NAME, String c_FIRST, String c_MIDDLE, String c_LAST, double c_BALANCE) {
        W_NAME = w_NAME;
        D_NAME = d_NAME;
        C_FIRST = c_FIRST;
        C_MIDDLE = c_MIDDLE;
        C_LAST = c_LAST;
        C_BALANCE = c_BALANCE;
    }
    
    public String getW_NAME() {
        return W_NAME;
    }
  
    public String getD_NAME() {
        return D_NAME;
    }

    public double getC_BALANCE() {
        return C_BALANCE;
    }

    public String getC_FIRST() {
        return C_FIRST;
    }
    
    public String getC_MIDDLE() {
        return C_MIDDLE;
    }
    
    public String getC_LAST() {
        return C_LAST;
    }

}
