package org.embulk.output.azure_blob_storage;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobType;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Exec;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.TransactionalFileOutput;
import org.slf4j.Logger;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.List;

public class AzureBlobStorageFileOutputPlugin
        implements FileOutputPlugin
{
    public interface PluginTask
            extends Task
    {
        @Config("account_name")
        String getAccountName();

        @Config("account_key")
        String getAccountKey();

        @Config("container")
        String getContainer();

        @Config("path_prefix")
        String getPathPrefix();

        @Config("file_ext")
        String getFileNameExtension();

        @Config("sequence_format")
        @ConfigDefault("\"%03d.%02d\"")
        String getSequenceFormat();

        @Config("max_connection_retry")
        @ConfigDefault("10") // 10 times retry to connect Azure Blob Storage if failed.
        int getMaxConnectionRetry();

        /**
         * [BLOCK_BLOB | PAGE_BLOB | APPEND_BLOB | UNSPECIFIED]
         */
        @Config("blob_type")
        @ConfigDefault("\"UNSPECIFIED\"")
        String getBlobType();
    }

    private static final Logger log = Exec.getLogger(AzureBlobStorageFileOutputPlugin.class);

    @Override
    public ConfigDiff transaction(ConfigSource config, int taskCount,
                                  FileOutputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        try {
            CloudBlobClient blobClient = newAzureClient(task.getAccountName(), task.getAccountKey());
            String containerName = task.getContainer();
            CloudBlobContainer container = blobClient.getContainerReference(containerName);
            if (!container.exists()) {
                log.info("container {} doesn't exist and is created.", containerName);
                container.createIfNotExists();
            }
        }
        catch (StorageException | URISyntaxException ex) {
            throw new ConfigException(ex);
        }

        return resume(task.dump(), taskCount, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource, int taskCount, FileOutputPlugin.Control control)
    {
        control.run(taskSource);

        return Exec.newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource, int taskCount, List<TaskReport> successTaskReports)
    {
    }

    private static CloudBlobClient newAzureClient(String accountName, String accountKey)
    {
        String connectionString = "DefaultEndpointsProtocol=https;" +
                "AccountName=" + accountName + ";" +
                "AccountKey=" + accountKey;

        CloudStorageAccount account;
        try {
            account = CloudStorageAccount.parse(connectionString);
        }
        catch (InvalidKeyException | URISyntaxException ex) {
            throw new ConfigException(ex.getMessage());
        }
        return account.createCloudBlobClient();
    }

    @Override
    public TransactionalFileOutput open(TaskSource taskSource, final int taskIndex)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);
        final BlobType blobType = BlobType.valueOf(task.getBlobType());
        final CloudBlobClient blobClient = newAzureClient(task.getAccountName(), task.getAccountKey());

        // should support multiple blob type in the future.
        switch (blobType) {
            case BLOCK_BLOB:
                return new BlockBlobFileOutput(blobClient, task, taskIndex);
            default:
                return new BlobFileOutput(blobClient, task, taskIndex);
        }
    }
}
