package importtask.cs4224c.task;

import com.google.common.collect.Lists;
import importtask.cs4224c.util.Constant;
import importtask.cs4224c.util.ProjectConfig;
import importtask.cs4224c.util.QueryExecutor;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Paths;

public class DropTableTask implements Runnable {

    private final Logger logger = LoggerFactory.getLogger(DropTableTask.class);

    public static void main(String[] args) {
        Runnable task = new DropTableTask();
        task.run();
        QueryExecutor.getInstance().closeConnection();
    }


    @Override
    public void run() {
        logger.info("DropTableTask begins.");

        FileInputStream dropTableFile = null;
        String dropTableSchema;
        ProjectConfig config = ProjectConfig.getInstance();
        try {
            dropTableFile = new FileInputStream(new File(Paths.get(config.getProjectRoot(), config.getDataModelFoler(), "drop.cql").toUri()));
            dropTableSchema = IOUtils.toString(dropTableFile, "UTF-8");
        } catch (Exception e) {
            logger.error("Cannot get drop table CQL {}", e);
            throw new RuntimeException();
        }

        String[] statements = dropTableSchema.split(Constant.STATEMENT_DECIMETER);
        QueryExecutor.getInstance().executeWithinKeyspace(Lists.newArrayList(statements), config.getCassandraKeyspace());

        logger.info("DropTableTask ends.");
    }
}
