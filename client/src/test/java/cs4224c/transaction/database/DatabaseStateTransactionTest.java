package cs4224c.transaction.database;

import cs4224c.parser.AbstractParser;
import cs4224c.transaction.AbstractTransaction;
import cs4224c.transaction.BaseTransactionTest;
import org.junit.Test;

import java.io.IOException;

public class DatabaseStateTransactionTest extends BaseTransactionTest {

    public DatabaseStateTransactionTest() {
        super(AbstractParser.class);
    }

    @Override
    protected AbstractTransaction executeFlowWithData(String dataFileName) throws Exception {
        throw new UnsupportedOperationException("This transaction is just to ouput database status");
    }


    @Test
    public void testDatabaseState() throws IOException {
        new DatabaseStateTransaction().executeFlow();
        this.validateSystemOutput("expectedOutput.txt");
    }
}
