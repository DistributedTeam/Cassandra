package client.cs4224c;

import client.cs4224c.parser.AbstractParser;
import client.cs4224c.parser.ParserMap;
import client.cs4224c.transaction.AbstractTransaction;
import client.cs4224c.transaction.database.DatabaseStateTransaction;
import client.cs4224c.util.Constant;
import client.cs4224c.util.ProjectConfig;
import client.cs4224c.util.QueryExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Paths;
import java.util.Scanner;

public class ExperimentClient {

    private static final Logger logger = LoggerFactory.getLogger(ExperimentClient.class);

    private static int INDEX_COMMAND = 0;

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            logger.warn("Expect input file name");
            System.exit(1);
        }

        logger.info("Transaction file name is " + args[0]);

        Scanner sc = new Scanner(Paths.get(ProjectConfig.getInstance().getProjectRoot(), ProjectConfig.getInstance().getTransactionFileFolder(),
                args[0]));

        int numOfTransaction = 0;

        long beginTime = System.currentTimeMillis();
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
            numOfTransaction++;
        }
        long endTime = System.currentTimeMillis();

        double executionTimeInSecond = (endTime - beginTime) / 1000.0;

        System.out.println("\n[SUMMARY]");
        System.out.println("Number of executed transactions: " + numOfTransaction);
        System.out.println("Total transaction execution time (seconds): " + executionTimeInSecond);
        System.out.println("Transaction throughput: " + executionTimeInSecond / numOfTransaction);

        // Output DB state
        System.out.println("\n[DATABASE STATE]");
        new DatabaseStateTransaction().execute();

        QueryExecutor.getInstance().closeConnection();
    }
}
