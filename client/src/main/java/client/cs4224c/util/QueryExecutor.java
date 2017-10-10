package client.cs4224c.util;

import client.cs4224c.policy.ExperimentLoadBalancePolicy;
import com.datastax.driver.core.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryExecutor {

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
        reload();
    }

    public void reload() {
        Cluster.Builder builder = Cluster.builder()
                .addContactPoint(ProjectConfig.getInstance().getCassandraIp())
                .withCompression(ProtocolOptions.Compression.LZ4)
                .withSocketOptions(
                    new SocketOptions()
                        .setReadTimeoutMillis(ProjectConfig.getInstance().getCassandraClientReadTimeout()));
        if (StringUtils.isNotEmpty(System.getProperty(Constant.PROPERTY_EXPERIMENT_HOST))) {
            builder.withLoadBalancingPolicy(new ExperimentLoadBalancePolicy());
        }
        cluster = builder.build();
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

    public ResultSet execute(String query) {
        return session.execute(query);
    }

    public void closeConnection() {
        if (cluster != null) {
            cluster.close();
        }
    }
}
