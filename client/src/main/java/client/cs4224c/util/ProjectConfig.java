package client.cs4224c.util;


import org.apache.commons.configuration2.CompositeConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;

public class ProjectConfig {

    private final static Logger logger = LoggerFactory.getLogger(ProjectConfig.class);

    private static final String CASSANDRA_IP = "cassandra.ip";
    private static final String CASSANDRA_KEYSPACE = "cassandra.keyspace";
    private static final String CASSANDRA_CLIENT_READ_TIMEOUT = "cassandra.client.read.timeout";
    private static final String TRANSACTION_FILE_FOLDER = "transaction.file.folder";
    private static final String QUERY_CONSISTENCY_LEVEL = "query.consistency.level";

    private static ProjectConfig instance;

    public static ProjectConfig getInstance() {
        if (instance == null) {
            instance = new ProjectConfig();
        }
        return instance;
    }

    private Configuration configuration;

    private ProjectConfig() {
        reload();
    }

    public ProjectConfig reload() {
        CompositeConfiguration compositeConfiguration = new CompositeConfiguration();

        if (Constant.ENV_TEST.equals(System.getProperty(Constant.PROPERTY_KEY_ENV))) {
            logger.info("Minions find we are in TEST environment, try to load test configuration which will override the main one");
            try {
                compositeConfiguration.addConfiguration(new Configurations().properties(Paths.get(System.getProperty("user.dir"), "project.test.properties").toFile()));
            } catch (ConfigurationException e) {
                logger.error("Load Teset Configuration Initialization Fail! {}", e);
                throw new RuntimeException();
            }
        }

        try {
            compositeConfiguration.addConfiguration(new Configurations().properties(Paths.get(System.getProperty("user.dir"), "project.properties").toFile()));
        } catch (ConfigurationException e) {
            logger.error("Load Main Configuration Initialization Fail! {}", e);
            throw new RuntimeException();
        }

        configuration = compositeConfiguration;
        return this;
    }

    public String getProjectRoot() {
        if (System.getProperty("user.dir") != null) {
            return Paths.get(System.getProperty("user.dir"), "..").toString();
        }
        throw new RuntimeException("Cannot find user.dir");
    }

    public String getCassandraIp() {
        return configuration.getString(CASSANDRA_IP);
    }

    public String getCassandraKeyspace() {
        return configuration.getString(CASSANDRA_KEYSPACE);
    }

    public String getTransactionFileFolder() {
        return configuration.getString(TRANSACTION_FILE_FOLDER);
    }

    public int getCassandraClientReadTimeout() {
        return configuration.getInt(CASSANDRA_CLIENT_READ_TIMEOUT);
    }

    public String getQueryConsistencyLevel() {
        return configuration.getString(QUERY_CONSISTENCY_LEVEL);
    }
}
