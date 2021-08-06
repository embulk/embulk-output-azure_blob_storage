package org.embulk.output.azure_blob_storage;

import com.google.common.collect.Lists;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.output.azure_blob_storage.AzureBlobStorageFileOutputPlugin.PluginTask;
import org.embulk.spi.Exec;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.FileOutputRunner;
import org.embulk.spi.Schema;
import org.embulk.standards.CsvParserPlugin;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import static org.embulk.output.azure_blob_storage.TestHelper.AZURE_ACCOUNT_KEY;
import static org.embulk.output.azure_blob_storage.TestHelper.AZURE_ACCOUNT_NAME;
import static org.embulk.output.azure_blob_storage.TestHelper.AZURE_CONTAINER;
import static org.embulk.output.azure_blob_storage.TestHelper.Control;
import static org.embulk.output.azure_blob_storage.TestHelper.config;
import static org.embulk.output.azure_blob_storage.TestHelper.deleteContainerIfExists;
import static org.embulk.output.azure_blob_storage.TestHelper.formatterConfig;
import static org.embulk.output.azure_blob_storage.TestHelper.inputConfig;
import static org.embulk.output.azure_blob_storage.TestHelper.isExistsContainer;
import static org.embulk.output.azure_blob_storage.TestHelper.parserConfig;
import static org.embulk.output.azure_blob_storage.TestHelper.schemaConfig;
import static org.junit.Assert.assertEquals;

public class TestAzureBlobStorageFileOutputPlugin
{
    private FileOutputRunner runner;

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();
    private AzureBlobStorageFileOutputPlugin plugin;

    @Before
    public void createResources()
    {
        plugin = new AzureBlobStorageFileOutputPlugin();
        runner = new FileOutputRunner(runtime.getInstance(AzureBlobStorageFileOutputPlugin.class));
    }

    @Test
    public void checkDefaultValues()
    {
        ConfigSource config = Exec.newConfigSource()
                                  .set("in", inputConfig())
                                  .set("parser", parserConfig(schemaConfig()))
                                  .set("type", "azure_blob_storage")
                                  .set("account_name", AZURE_ACCOUNT_NAME)
                                  .set("account_key", AZURE_ACCOUNT_KEY)
                                  .set("container", AZURE_CONTAINER)
                                  .set("path_prefix", "my-prefix")
                                  .set("file_ext", ".csv")
                                  .set("formatter", formatterConfig());

        PluginTask task = config.loadConfig(PluginTask.class);

        assertEquals(AZURE_ACCOUNT_NAME, task.getAccountName());
        assertEquals(AZURE_ACCOUNT_KEY, task.getAccountKey());
        assertEquals(AZURE_CONTAINER, task.getContainer());
        assertEquals(10, task.getMaxConnectionRetry());
    }

    @Test
    public void testTransaction()
    {
        ConfigSource config = Exec.newConfigSource()
                                  .set("in", inputConfig())
                                  .set("parser", parserConfig(schemaConfig()))
                                  .set("type", "azure_blob_storage")
                                  .set("account_name", AZURE_ACCOUNT_NAME)
                                  .set("account_key", AZURE_ACCOUNT_KEY)
                                  .set("container", AZURE_CONTAINER)
                                  .set("path_prefix", "my-prefix")
                                  .set("file_ext", ".csv")
                                  .set("formatter", formatterConfig());

        Schema schema = config.getNested("parser").loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();

        runner.transaction(config, schema, 0, new Control());
    }

    @Test
    public void testTransactionCreateNonexistsContainer() throws Exception
    {
        // Azure Container can't be created 30 seconds after deletion.
        String container = "non-exists-container-" + System.currentTimeMillis();
        deleteContainerIfExists(container);

        assertEquals(false, isExistsContainer(container));

        ConfigSource config = Exec.newConfigSource()
                                  .set("in", inputConfig())
                                  .set("parser", parserConfig(schemaConfig()))
                                  .set("type", "azure_blob_storage")
                                  .set("account_name", AZURE_ACCOUNT_NAME)
                                  .set("account_key", AZURE_ACCOUNT_KEY)
                                  .set("container", container)
                                  .set("path_prefix", "my-prefix")
                                  .set("file_ext", ".csv")
                                  .set("formatter", formatterConfig());

        Schema schema = config.getNested("parser").loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();

        runner.transaction(config, schema, 0, new Control());

        assertEquals(true, isExistsContainer(container));
        deleteContainerIfExists(container);
    }

    @Test
    public void testResume()
    {
        PluginTask task = config().loadConfig(PluginTask.class);
        ConfigDiff configDiff = plugin.resume(task.dump(), 0, new FileOutputPlugin.Control()
        {
            @Override
            public List<TaskReport> run(TaskSource taskSource)
            {
                return Lists.newArrayList(Exec.newTaskReport());
            }
        });
        //assertEquals("in/aa/a", configDiff.get(String.class, "last_path"));
    }

    @Test
    public void testCleanup()
    {
        PluginTask task = config().loadConfig(PluginTask.class);
        plugin.cleanup(task.dump(), 0, Lists.<TaskReport>newArrayList()); // no errors happens
    }

    @Test(expected = RuntimeException.class)
    public void testCreateAzureClientThrowsConfigException()
    {
        ConfigSource config = Exec.newConfigSource()
                                  .set("in", inputConfig())
                                  .set("parser", parserConfig(schemaConfig()))
                                  .set("type", "azure_blob_storage")
                                  .set("account_name", "invalid-account-name")
                                  .set("account_key", AZURE_ACCOUNT_KEY)
                                  .set("container", AZURE_CONTAINER)
                                  .set("path_prefix", "my-prefix")
                                  .set("file_ext", ".csv")
                                  .set("formatter", formatterConfig());

        Schema schema = config.getNested("parser").loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();

        runner.transaction(config, schema, 0, new Control());
    }
}
