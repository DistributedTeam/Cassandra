package client.cs4224c;

import client.cs4224c.parser.AbstractParser;
import client.cs4224c.parser.ParserMap;
import client.cs4224c.transaction.AbstractTransaction;
import client.cs4224c.util.Constant;
import client.cs4224c.util.ProjectConfig;
import client.cs4224c.util.QueryExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.Scanner;

public class ExperimentClient {

    private static final Logger logger = LoggerFactory.getLogger(ExperimentClient.class);

    private static int INDEX_COMMAND = 0;

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            logger.warn("Expect input file name and host name");
            System.exit(1);
        }

        logger.info("Requested host is {}", args[1]);

        System.setProperty(Constant.PROPERTY_EXPERIMENT_HOST, args[1]);

        logger.info("Transaction file name is {}", args[0]);

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

        System.err.println("\n[SUMMARY]");
        System.err.println("Number of executed transactions: " + numOfTransaction);
        System.err.println("Total transaction execution time (seconds): " + executionTimeInSecond);
        System.err.println("Transaction throughput: " + executionTimeInSecond / numOfTransaction);

        QueryExecutor.getInstance().closeConnection();
    }
}
