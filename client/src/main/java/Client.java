import parser.IParser;
import parser.ParserMap;
import transaction.ITransaction;
import cs4224c.util.Constant;

import java.util.Scanner;

public class Client {

    private static int INDEX_COMMAND = 0;

    public static void main(String[] args) throws Exception {

        Scanner sc = new Scanner(System.in);

        while (sc.hasNextLine()) {
            String line = sc.nextLine();
            String[] arguments = line.split(Constant.COMMA_SEPARATOR);
            String command = arguments[INDEX_COMMAND];

            Class<? extends IParser> parserClass = ParserMap.get(command);
            if (parserClass == null) {
                System.out.println("Invalid command: " + command);
            }

            IParser parser = parserClass.newInstance();
            ITransaction transaction = parser.parse(sc, arguments);

            System.out.println(transaction);
        }


    }
}
