package org.embulk.output.azure_blob_storage;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigSource;
import org.embulk.spi.Buffer;
import org.embulk.spi.FileOutputRunner;
import org.embulk.spi.Schema;
import org.embulk.standards.CsvParserPlugin;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.List;

import static org.embulk.output.azure_blob_storage.TestHelper.AZURE_ACCOUNT_KEY;
import static org.embulk.output.azure_blob_storage.TestHelper.AZURE_ACCOUNT_NAME;
import static org.embulk.output.azure_blob_storage.TestHelper.AZURE_CONTAINER;
import static org.embulk.output.azure_blob_storage.TestHelper.AZURE_PATH_PREFIX;
import static org.embulk.output.azure_blob_storage.TestHelper.Control;
import static org.embulk.output.azure_blob_storage.TestHelper.config;
import static org.embulk.output.azure_blob_storage.TestHelper.convertInputStreamToByte;
import static org.embulk.output.azure_blob_storage.TestHelper.getFileContentsFromAzure;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeNotNull;

public class TestBlockBlobFileOutput
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();
    private AzureBlobStorageFileOutputPlugin plugin;
    private FileOutputRunner runner;

    @BeforeClass
    public static void init()
    {
        assumeNotNull(AZURE_ACCOUNT_NAME, AZURE_ACCOUNT_KEY, AZURE_CONTAINER);
    }

    @Before
    public void setup()
    {
        plugin = new AzureBlobStorageFileOutputPlugin();
        runner = new FileOutputRunner(runtime.getInstance(AzureBlobStorageFileOutputPlugin.class));
    }

    private ConfigSource getBlockBlobConfig()
    {
        return config().set("blob_type", "BLOCK_BLOB");
    }
    @Test
    public void testSingleBlock() throws Exception
    {
        ConfigSource configSource = getBlockBlobConfig();
        AzureBlobStorageFileOutputPlugin.PluginTask task = configSource.loadConfig(AzureBlobStorageFileOutputPlugin.PluginTask.class);
        Schema schema = configSource.getNested("parser").loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();
        runner.transaction(configSource, schema, 0, new Control());

        BlockBlobFileOutput output = (BlockBlobFileOutput) plugin.open(task.dump(), 0);
        output.nextFile();

        FileInputStream is = new FileInputStream(Resources.getResource("one_record.csv").getPath());
        byte[] bytes = convertInputStreamToByte(is);
        Buffer buffer = Buffer.wrap(bytes);
        output.add(buffer);

        output.finish();
        output.commit();

        String remotePath = AZURE_PATH_PREFIX + String.format(task.getSequenceFormat(), 0, 0) + task.getFileNameExtension();

        ImmutableList<List<String>> records = getFileContentsFromAzure(remotePath);
        assertEquals(1, records.size());
        List<String> record = records.get(0);
        assertEquals("1", record.get(0));
        assertEquals("32864", record.get(1));
        assertEquals("2015-01-27 19:23:49", record.get(2));
        assertEquals("20150127", record.get(3));
        assertEquals("embulk", record.get(4));
        assertEquals("{\"k\":true}", record.get(5));
    }

    @Test
    public void testMultipleBlocks() throws Exception
    {
        ConfigSource configSource = getBlockBlobConfig();
        AzureBlobStorageFileOutputPlugin.PluginTask task = configSource.loadConfig(AzureBlobStorageFileOutputPlugin.PluginTask.class);
        Schema schema = configSource.getNested("parser").loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();
        runner.transaction(configSource, schema, 0, new Control());

        BlockBlobFileOutput output = (BlockBlobFileOutput) plugin.open(task.dump(), 0);
        // set small block size to check for multiple blocks upload
        Field blockSize = output.getClass().getDeclaredField("blockSize");
        blockSize.setAccessible(true);
        blockSize.set(output, 200); // flush for 200 bytes file

        output.nextFile();

        FileInputStream is = new FileInputStream(Resources.getResource("one_record.csv").getPath());
        byte[] bytes = convertInputStreamToByte(is);
        Buffer buffer = Buffer.wrap(bytes);
        for (int i = 0; i < 100; i++) {
            Field outputField = output.getClass().getDeclaredField("output");
            outputField.setAccessible(true);

            output.add(buffer);
            ((OutputStream) outputField.get(output)).flush();
        }

        output.finish();
        output.commit();

        String remotePath = AZURE_PATH_PREFIX + String.format(task.getSequenceFormat(), 0, 0) + task.getFileNameExtension();

        ImmutableList<List<String>> records = getFileContentsFromAzure(remotePath);
        assertEquals(100, records.size());
        for (int i = 0; i < 100; i++) {
            List<String> record = records.get(i);
            assertEquals("1", record.get(0));
            assertEquals("32864", record.get(1));
            assertEquals("2015-01-27 19:23:49", record.get(2));
            assertEquals("20150127", record.get(3));
            assertEquals("embulk", record.get(4));
            assertEquals("{\"k\":true}", record.get(5));
        }
    }
}
