package cs4224c;

import com.datastax.driver.core.ResultSet;
import com.google.common.collect.Lists;
import cs4224c.task.CreateKeyspaceTask;
import cs4224c.task.DropTableTask;
import cs4224c.task.ImportDataTask;
import cs4224c.task.ImportSchemaTask;
import cs4224c.util.Constant;
import cs4224c.util.ProjectConfig;
import cs4224c.util.QueryExecutor;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FilenameUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

public class ImportTest {

    private final Logger logger = LoggerFactory.getLogger(ImportTest.class);

    private ProjectConfig config;

    @Before
    public void before() {
        System.setProperty(Constant.PROPERTY_KEY_ENV, Constant.ENV_TEST);
        config = ProjectConfig.getInstance().reload();
    }

    @Test
    public void testImportData() throws Exception {
        logger.info("Begin to import data.");

        logger.info("Run create keyspace task");
        new CreateKeyspaceTask().run();

        logger.info("Run drop table task");
        new DropTableTask().run();

        logger.info("Run import schema task");
        new ImportSchemaTask().run();

        logger.info("Run Import Data Task");
        new ImportDataTask().run();

        validateDatabase();
    }

    private void validateDatabase() throws Exception {
        logger.info("Verifying data table size in DB");

        validateTable("customer.csv");
        validateTable("order_by_o_id.csv");
        validateTable("order_line_item.csv");
        validateTable("stock_item.csv");
        validateTable("warehouse_district.csv");
        validateTable("delivery_transaction.csv");
    }

    public void validateTable(String tableFile) throws Exception {
        logger.info("Validate Table {}", tableFile);

        String tableName = FilenameUtils.getBaseName(tableFile);
        ResultSet resultSet = QueryExecutor.getInstance().executeWithinKeyspace(String.format("SELECT COUNT(*) from %s", tableName), config.getCassandraKeyspace());

        List<CSVRecord> csvRecords = readRecord(tableFile);

        Assert.assertEquals("DB and CSV file should have the same number of rows." + tableFile, csvRecords.size(), resultSet.one().getLong(0));
    }

    public List<CSVRecord> readRecord(String name) throws IOException {
        logger.info("Read csv file from {}", name);

        ProjectConfig config = ProjectConfig.getInstance();
        return Lists.newLinkedList(CSVFormat.INFORMIX_UNLOAD_CSV.parse(new FileReader(Paths.get(config.getProjectRoot(), config.getDataCsvFolder(), name).toFile())));
    }
}
