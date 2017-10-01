package importtask.cs4224c.task;

import com.google.common.collect.Lists;
import importtask.cs4224c.util.ProjectConfig;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;

public class ImportDataTask implements Runnable {

    private final Logger logger = LoggerFactory.getLogger(DropTableTask.class);

    public static void main(String[] args) {
        Runnable task = new ImportDataTask();
        task.run();
    }

    @Override
    public void run() {
        logger.info("ImportDataTask begins.");
        ProjectConfig config = ProjectConfig.getInstance();

        String importDataCqlTemplate;
        try {
            importDataCqlTemplate = IOUtils.toString(this.getClass().getResourceAsStream("importData.cql"));
        } catch (IOException e) {
            logger.error("Cannot get template importData");
            throw new RuntimeException();
        }
        logger.info("importData template {}", importDataCqlTemplate);

        String importDataCql = importDataCqlTemplate.replace("_DATA_CSV_FOLDER_",
                Paths.get(config.getProjectRoot(), config.getDataCsvFolder()).toString() + File.separator)
                .replace("\n", ""); // delete all line-breaker
        logger.info("importData cql {}", importDataCql);

        String[] cmd = new String[] {
                getCqlShCmd(),
                "-k",
                config.getCassandraKeyspace(),
                "-e",
                importDataCql,
                config.getCassandraIp()
        };
        logger.info("Command will be executed {}", Lists.newArrayList(cmd));

        Process process = null;
        try {
            ProcessBuilder processBuilder = new ProcessBuilder()
                    .command(cmd)
                    .redirectOutput(ProcessBuilder.Redirect.INHERIT);
            Map<String, String> env = processBuilder.environment();
            env.put("TZ", "UTC"); // standardize the timezone UTC or cqlsh will detect local time which is bad
            process = processBuilder.start();

            process.waitFor();
            String error = IOUtils.toString(process.getErrorStream());

            if (StringUtils.isNotEmpty(error)) {
                logger.warn("Notice that there are errors during importing data. Usually, these error will be corrected by cqlsh with retry. " +
                        "Please check whether they are outstanding or not: {}", error);
            }
        } catch (Exception e) {
            logger.error("Cannot execute command and get result {}", e);
            throw new RuntimeException();
        }


        logger.info("ImportDataTask ends");
    }

    private String getCqlShCmd() {
        String OS = System.getProperty("os.name");
        if (OS == null) {
            logger.error("Unsupported OS");
            throw new RuntimeException();
        }
        if (OS.startsWith("Mac") || OS.startsWith("Linux")) {
            logger.info("Current OS is {}, use cqlsh", OS);
            return Paths.get(System.getProperty("user.dir"), "cassandraCqlSh", "bin", "cqlsh").toString();
        }
        if (OS.startsWith("Windows")) {
            logger.info("Current OS is {}, use cqlsh.bat", OS);
            return Paths.get(System.getProperty("user.dir"), "cassandraCqlSh", "bin", "cqlsh.bat").toString();
        }
        logger.error("Unsupported OS {}", OS);
        throw new RuntimeException();
    }
}
