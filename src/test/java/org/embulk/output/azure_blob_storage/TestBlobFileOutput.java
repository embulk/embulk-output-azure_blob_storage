package org.embulk.output.azure_blob_storage;

import com.google.common.collect.ImmutableList;
import org.embulk.EmbulkSystemProperties;
import org.embulk.config.ConfigSource;
import org.embulk.formatter.csv.CsvFormatterPlugin;
import org.embulk.input.file.LocalFileInputPlugin;
import org.embulk.parser.csv.CsvParserPlugin;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.FileOutputRunner;
import org.embulk.spi.FormatterPlugin;
import org.embulk.spi.ParserPlugin;
import org.embulk.spi.Schema;
import org.embulk.test.TestingEmbulk;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;

import static org.embulk.output.azure_blob_storage.AzureBlobStorageFileOutputPlugin.CONFIG_MAPPER;
import static org.embulk.output.azure_blob_storage.TestHelper.AZURE_ACCOUNT_KEY;
import static org.embulk.output.azure_blob_storage.TestHelper.AZURE_ACCOUNT_NAME;
import static org.embulk.output.azure_blob_storage.TestHelper.AZURE_CONTAINER;
import static org.embulk.output.azure_blob_storage.TestHelper.AZURE_PATH_PREFIX;
import static org.embulk.output.azure_blob_storage.TestHelper.config;
import static org.embulk.output.azure_blob_storage.TestHelper.getFileContentsFromAzure;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeNotNull;

public class TestBlobFileOutput
{
    private static final EmbulkSystemProperties EMBULK_SYSTEM_PROPERTIES = EmbulkSystemProperties.of(new Properties());

    @Rule
    public TestingEmbulk embulk = TestingEmbulk.builder()
        .setEmbulkSystemProperties(EMBULK_SYSTEM_PROPERTIES)
        .registerPlugin(ParserPlugin.class, "csv", CsvParserPlugin.class)
        .registerPlugin(FormatterPlugin.class, "csv", CsvFormatterPlugin.class)
        .registerPlugin(FileOutputPlugin.class, "azure_blob_storage", AzureBlobStorageFileOutputPlugin.class)
        .registerPlugin(FileInputPlugin.class, "file", LocalFileInputPlugin.class)
        .build();

    private FileOutputRunner runner;

    @BeforeClass
    public static void init()
    {
        assumeNotNull(AZURE_ACCOUNT_NAME, AZURE_ACCOUNT_KEY, AZURE_CONTAINER);
    }

    @Before
    public void setup()
    {
        AzureBlobStorageFileOutputPlugin plugin = new AzureBlobStorageFileOutputPlugin();
        runner = new FileOutputRunner(plugin);
    }

    @Test
    public void testAzureFileOutputByOpen()
        throws Exception
    {
        ConfigSource configSource = config();
        AzureBlobStorageFileOutputPlugin.PluginTask task = CONFIG_MAPPER.map(configSource, AzureBlobStorageFileOutputPlugin.PluginTask.class);
        Schema schema = CONFIG_MAPPER.map(configSource.getNested("parser"), CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();
        try {
            TestingEmbulk.RunResult result = embulk.runOutput(configSource, Paths.get(getClass().getClassLoader().getResource("sample_with_column.csv").getPath()));
        }
        catch (IOException ioException) {
            fail(ioException.getMessage());
        }

        String remotePath = AZURE_PATH_PREFIX + String.format(task.getSequenceFormat(), 0, 0) + task.getFileNameExtension();
        assertRecords(remotePath);
    }

    @Test
    public void estAzureFileOutputByOpenWithNonReadableFile()
        throws Exception
    {
        ConfigSource configSource = config();
        AzureBlobStorageFileOutputPlugin.PluginTask task = CONFIG_MAPPER.map(configSource, AzureBlobStorageFileOutputPlugin.PluginTask.class);
        Schema schema = CONFIG_MAPPER.map(configSource.getNested("parser"), CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();

        Field field = BlobFileOutput.class.getDeclaredField("file");
        field.setAccessible(true);

        try {
            TestingEmbulk.RunResult result = embulk.runOutput(configSource, Paths.get(getClass().getClassLoader().getResource("sample_with_column.csv").getPath()));
        }
        catch (IOException ioException) {
            fail(ioException.getMessage());
        }
    }

    @Test
    public void testAzureFileOutputByOpenWithRetry()
        throws Exception
    {
        ConfigSource configSource = config();
        AzureBlobStorageFileOutputPlugin.PluginTask task = CONFIG_MAPPER.map(configSource, AzureBlobStorageFileOutputPlugin.PluginTask.class);
        Schema schema = CONFIG_MAPPER.map(configSource.getNested("parser"), CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();
        try {
            TestingEmbulk.RunResult result = embulk.runOutput(configSource, Paths.get(getClass().getClassLoader().getResource("sample_with_column.csv").getPath()));
        }
        catch (IOException ioException) {
            fail(ioException.getMessage());
        }
    }

    private void assertRecords(String azurePath)
        throws Exception
    {
        ImmutableList<List<String>> records = getFileContentsFromAzure(azurePath);
        assertEquals(5, records.size());
        {
            List<String> record = records.get(0);
            assertEquals("1", record.get(0));
            assertEquals("32864", record.get(1));
            assertEquals("2015-01-27 19:23:49", record.get(2));
            assertEquals("20150127", record.get(3));
            assertEquals("embulk", record.get(4));
            assertEquals("\"{\"\"k\"\":true}\"", record.get(5));
        }

        {
            List<String> record = records.get(1);
            assertEquals("2", record.get(0));
            assertEquals("14824", record.get(1));
            assertEquals("2015-01-27 19:01:23", record.get(2));
            assertEquals("20150127", record.get(3));
            assertEquals("embulk jruby", record.get(4));
            assertEquals("\"{\"\"k\"\":1}\"", record.get(5));
        }

        {
            List<String> record = records.get(2);
            assertEquals("\"{\"\"k\"\":1.23}\"", record.get(5));
        }

        {
            List<String> record = records.get(3);
            assertEquals("\"{\"\"k\"\":\"\"v\"\"}\"", record.get(5));
        }

        {
            List<String> record = records.get(4);
            assertEquals("\"{\"\"k\"\":\"\"2015-02-03 08:13:45\"\"}\"", record.get(5));
        }
    }
}
