package cs4224c.util;

import com.datastax.driver.core.*;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class QueryExecutor {

    private final String GLOBAL_SESSION = "_global";

    final Logger logger = LoggerFactory.getLogger(QueryExecutor.class);

    private static QueryExecutor instance;

    private Map<PStatement, PreparedStatement> statementMap = new HashMap<>();

    public synchronized static QueryExecutor getInstance() {
        if (instance == null) {
            instance = new QueryExecutor();
        }
        return instance;
    }

    private Cluster cluster;

    private Session session;

    private QueryExecutor() {
        cluster = Cluster.builder()
                .addContactPoint(ProjectConfig.getInstance().getCassandraIp())
                .build();
        session = cluster.connect(ProjectConfig.getInstance().getCassandraKeyspace());
        initialize();
    }

    private void initialize() {
        logger.info("Send all prepared statements to server now.");
        for (PStatement statement : PStatement.values()) {
            statementMap.put(statement, session.prepare(statement.getCql()));
        }
    }

    public ResultSet execute(PStatement statement, List<Object> args) {
        logger.info("About to execute preparedStatement {} with args {}", statement, args);
        if (!statementMap.containsKey(statement)) {
            logger.error("Does not find any matched prepared statements. You may forget the initialization.");
            throw new RuntimeException();
        }
        BoundStatement boundStatement = statementMap.get(statement).bind();
        boundStatement.bind(args.toArray(new Object[args.size()]));
        return session.execute(boundStatement);
    }

    public Row executeAndGetOneRow(PStatement statement, List<Object> args) {
        ResultSet resultSet = this.execute(statement, args);
        return resultSet.one();
    }

    public Row getAndUpdateWithRetry(PStatement getStatement, List<Object> getArgs, PStatement updateStatement, List<Object> updateArgs, Function<Row, List<Object>> updateFunc, Function<Row, Object> ifPositionFunc) {
        Row result = null;
        int count = 1;
        while (true) {
            result = executeAndGetOneRow(getStatement, getArgs);
            List<Object> newUpdateArgs = Lists.newArrayList();
            newUpdateArgs.addAll(updateFunc.apply(result));
            newUpdateArgs.addAll(updateArgs);
            newUpdateArgs.add(ifPositionFunc.apply(result)); // if statement params
            ResultSet resultSet = execute(updateStatement, newUpdateArgs);
            if (resultSet.wasApplied()) {
                break;
            }
            if (count >= ProjectConfig.getInstance().getMaxIfUpdateTry()) {
                logger.error("Executing getAndUpdateWithRetry with getStatement {} ({}) updateStatement {}({}) fails. Exceed maximum retry: {}",
                        getStatement, getArgs, updateStatement, updateArgs, count);
                throw new RuntimeException();
            }
            count++;
        }
        return result;
    }

    // USED IN TEST ENV

    public ResultSet execute(String query) {
        return session.execute(query);
    }

    public void closeConnection() {
        if (cluster != null) {
            cluster.close();
        }
    }
}
