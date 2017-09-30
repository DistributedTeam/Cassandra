package cs4224c.task;

import com.google.common.collect.Lists;
import cs4224c.util.Constant;
import cs4224c.util.ProjectConfig;
import cs4224c.util.QueryExecutor;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

public class ImportSchemaTask implements Runnable {

    private final Logger logger = LoggerFactory.getLogger(ImportSchemaTask.class);

    public static void main(String[] args) {
        Runnable task = new ImportSchemaTask();
        task.run();
        QueryExecutor.getInstance().closeConnection();
    }


    @Override
    public void run() {
        logger.info("ImportSchemaTask begins.");

        FileInputStream schemaFile = null;
        String schema;
        ProjectConfig config = ProjectConfig.getInstance();
        try {
            schemaFile = new FileInputStream(new File(Paths.get(config.getProjectRoot(), config.getDataModelFoler(), "schema.cql").toUri()));
            schema = IOUtils.toString(schemaFile, "UTF-8");
        } catch (Exception e) {
            logger.error("Cannot get schema {}", e);
            throw new RuntimeException();
        }

        String[] statements = schema.split(Constant.STATEMENT_DECIMETER);
        QueryExecutor.getInstance().executeWithinKeyspace(Lists.newArrayList(statements), config.getCassandraKeyspace());

        logger.info("ImportSchemaTask ends.");
    }
}
