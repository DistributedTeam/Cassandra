package client.cs4224c.parser;

import client.cs4224c.transaction.AbstractTransaction;
import client.cs4224c.transaction.database.DatabaseStateTransaction;
import client.cs4224c.transaction.topbalance.TopBalanceTransaction;

import java.util.Scanner;

public class DatabaseStateParser extends AbstractParser {

    @Override
    public AbstractTransaction parse(Scanner sc, String[] arguments) {
        return new DatabaseStateTransaction();
    }

}
