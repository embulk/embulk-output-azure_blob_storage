package org.embulk.output.azure_blob_storage;

import com.google.common.collect.ImmutableList;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigSource;
import org.embulk.spi.Buffer;
import org.embulk.spi.Exec;
import org.embulk.spi.FileOutputRunner;
import org.embulk.spi.Schema;
import org.embulk.standards.CsvParserPlugin;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.lang.reflect.Field;
import java.util.List;

import static org.embulk.output.azure_blob_storage.TestHelper.AZURE_PATH_PREFIX;
import static org.embulk.output.azure_blob_storage.TestHelper.LOCAL_PATH_PREFIX;
import static org.embulk.output.azure_blob_storage.TestHelper.config;
import static org.embulk.output.azure_blob_storage.TestHelper.convertInputStreamToByte;
import static org.embulk.output.azure_blob_storage.TestHelper.getFileContentsFromAzure;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestBlobFileOutput
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();
    private AzureBlobStorageFileOutputPlugin plugin;
    private FileOutputRunner runner;

    @Before
    public void setup()
    {
        plugin = new AzureBlobStorageFileOutputPlugin();
        runner = new FileOutputRunner(runtime.getInstance(AzureBlobStorageFileOutputPlugin.class));
    }

    @Test
    public void testAzureFileOutputByOpen() throws Exception
    {
        ConfigSource configSource = config();
        AzureBlobStorageFileOutputPlugin.PluginTask task = configSource.loadConfig(AzureBlobStorageFileOutputPlugin.PluginTask.class);
        Schema schema = configSource.getNested("parser").loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();
        runner.transaction(configSource, schema, 0, new TestHelper.Control());

        BlobFileOutput output = (BlobFileOutput) plugin.open(task.dump(), 0);

        output.nextFile();

        FileInputStream is = new FileInputStream(LOCAL_PATH_PREFIX);
        byte[] bytes = convertInputStreamToByte(is);
        Buffer buffer = Buffer.wrap(bytes);
        output.add(buffer);

        output.finish();
        output.commit();

        String remotePath = AZURE_PATH_PREFIX + String.format(task.getSequenceFormat(), 0, 0) + task.getFileNameExtension();
        assertRecords(remotePath);
        assertTrue(!output.isTempFileExist());
    }

    @Test
    public void testAzureFileOutputByOpenWithNonReadableFile() throws Exception
    {
        ConfigSource configSource = config();
        AzureBlobStorageFileOutputPlugin.PluginTask task = configSource.loadConfig(AzureBlobStorageFileOutputPlugin.PluginTask.class);
        Schema schema = configSource.getNested("parser").loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();
        runner.transaction(configSource, schema, 0, new TestHelper.Control());

        BlobFileOutput output = (BlobFileOutput) plugin.open(task.dump(), 0);

        output.nextFile();

        FileInputStream is = new FileInputStream(LOCAL_PATH_PREFIX);
        byte[] bytes = convertInputStreamToByte(is);
        Buffer buffer = Buffer.wrap(bytes);
        output.add(buffer);

        Field field = BlobFileOutput.class.getDeclaredField("file");
        field.setAccessible(true);
        File file = Exec.getTempFileSpace().createTempFile();
        file.setReadable(false);
        field.set(output, file);
        try {
            output.finish();
        }
        catch (Exception ex) {
            assertEquals(FileNotFoundException.class, ex.getCause().getClass());
            assertTrue(!output.isTempFileExist());
        }
    }

    @Test
    public void testAzureFileOutputByOpenWithRetry() throws Exception
    {
        ConfigSource configSource = config();
        AzureBlobStorageFileOutputPlugin.PluginTask task = configSource.loadConfig(AzureBlobStorageFileOutputPlugin.PluginTask.class);
        Schema schema = configSource.getNested("parser").loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();
        runner.transaction(configSource, schema, 0, new TestHelper.Control());

        BlobFileOutput output = (BlobFileOutput) plugin.open(task.dump(), 0);

        output.nextFile();

        FileInputStream is = new FileInputStream(LOCAL_PATH_PREFIX);
        byte[] bytes = convertInputStreamToByte(is);
        Buffer buffer = Buffer.wrap(bytes);
        output.add(buffer);

        // set maxConnectionRetry = 1 for Test
        Field maxConnectionRetry = BlobFileOutput.class.getDeclaredField("maxConnectionRetry");
        maxConnectionRetry.setAccessible(true);
        maxConnectionRetry.set(output, 1);

        try {
            output.finish();
        }
        catch (RuntimeException e) {
            assertTrue(!output.isTempFileExist());
        }
    }

    private void assertRecords(String azurePath) throws Exception
    {
        ImmutableList<List<String>> records = getFileContentsFromAzure(azurePath);
        assertEquals(6, records.size());
        {
            List<String> record = records.get(1);
            assertEquals("1", record.get(0));
            assertEquals("32864", record.get(1));
            assertEquals("2015-01-27 19:23:49", record.get(2));
            assertEquals("20150127", record.get(3));
            assertEquals("embulk", record.get(4));
            assertEquals("{\"k\":true}", record.get(5));
        }

        {
            List<String> record = records.get(2);
            assertEquals("2", record.get(0));
            assertEquals("14824", record.get(1));
            assertEquals("2015-01-27 19:01:23", record.get(2));
            assertEquals("20150127", record.get(3));
            assertEquals("embulk jruby", record.get(4));
            assertEquals("{\"k\":1}", record.get(5));
        }

        {
            List<String> record = records.get(3);
            assertEquals("{\"k\":1.23}", record.get(5));
        }

        {
            List<String> record = records.get(4);
            assertEquals("{\"k\":\"v\"}", record.get(5));
        }

        {
            List<String> record = records.get(5);
            assertEquals("{\"k\":\"2015-02-03 08:13:45\"}", record.get(5));
        }
    }
}
