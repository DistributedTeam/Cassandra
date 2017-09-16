package cs4224c.util;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryExecutor {

    private final String GLOBAL_SESSION = "_global";

    final Logger logger = LoggerFactory.getLogger(QueryExecutor.class);

    private static QueryExecutor instance;

    public static QueryExecutor getInstance() {
        if (instance == null) {
            instance = new QueryExecutor();
        }
        return instance;
    }

    private Cluster cluster;
    private Map<String, Session> sessionMap = new HashMap<>();

    private QueryExecutor() {
        try {
            cluster = Cluster.builder()
                    .addContactPoint(ProjectConfig.getInstance().getCassandraIp())
                    .build();
            sessionMap.put(GLOBAL_SESSION, cluster.connect());
        } catch (Exception e) {
            logger.error("Initialize drive ends with exception {}", e);
            throw new RuntimeException();
        }
    }

    private void initializeKeyspace(String keyspace) {
        try {
            sessionMap.put(keyspace, cluster.connect(keyspace));
        } catch (Exception e) {
            logger.error("Initialize session with keyspace {} ends with exception {}", keyspace, e);
            throw new RuntimeException();
        }
    }

    public ResultSet executeGlobally(String query) {
        logger.info("About to execute global query {}", query);
        return sessionMap.get(GLOBAL_SESSION).execute(query);
    }

    public ResultSet executeWithinKeyspace(String query, String keyspace) {
        logger.info("About to execute query in keyspace [{}] {} ", keyspace, query);
        Session session = sessionMap.get(keyspace);
        if (session == null) {
            initializeKeyspace(keyspace);
        }
        return sessionMap.get(keyspace).execute(query);
    }

    public void executeWithinKeyspace(List<String> queries, String keyspace) {
        for (String query : queries) {
            executeWithinKeyspace(query, keyspace);
        }
    }

    public void closeConnection() {
        if (cluster != null) {
            cluster.close();
        }
    }
}
