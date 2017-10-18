package client.cs4224c;

import client.cs4224c.parser.AbstractParser;
import client.cs4224c.parser.ParserMap;
import client.cs4224c.transaction.AbstractTransaction;
import client.cs4224c.util.Constant;
import client.cs4224c.util.QueryExecutor;

import java.util.Scanner;

public class Client {

    private static int INDEX_COMMAND = 0;

    public static void main(String[] args) throws Exception {

        Scanner sc = new Scanner(System.in);

        while (sc.hasNextLine()) {
            String line = sc.nextLine();
            String[] arguments = line.split(Constant.COMMA_SEPARATOR);
            String command = arguments[INDEX_COMMAND];

            Class<? extends AbstractParser> parserClass = ParserMap.get(command);
            if (parserClass == null) {
                System.out.println("Invalid command: " + command);
            }

            AbstractParser parser = parserClass.newInstance();
            AbstractTransaction transaction = parser.parse(sc, arguments);

            transaction.execute();
        }

        QueryExecutor.getInstance().closeConnection();
    }
}
