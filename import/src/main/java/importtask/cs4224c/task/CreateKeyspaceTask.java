package importtask.cs4224c.task;

import com.datastax.driver.core.ResultSet;
import importtask.cs4224c.util.ProjectConfig;
import importtask.cs4224c.util.QueryExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateKeyspaceTask implements Runnable {

    private final Logger logger = LoggerFactory.getLogger(CreateKeyspaceTask.class);

    public static void main(String[] args) {
        Runnable task = new CreateKeyspaceTask();
        task.run();
        QueryExecutor.getInstance().closeConnection();
    }

    @Override
    public void run() {
        logger.info("Execute create keyspace task");

        QueryExecutor queryExecutor = QueryExecutor.getInstance();
        ProjectConfig config = ProjectConfig.getInstance();

        String query = String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': '%s', 'replication_factor' : %s}",
                config.getCassandraKeyspace(), config.getCassandraReplicationClass(), config.getCassandraReplicationFactor());

        ResultSet resultSet = queryExecutor.executeGlobally(query);

        logger.info("Create keyspace task ends, resultSet {}", resultSet);
    }
}
