# Azure Blob Storage file output plugin for Embulk
[![Build Status](https://travis-ci.org/embulk/embulk-output-azure_blob_storage.svg?branch=master)](https://travis-ci.org/embulk/embulk-output-azure_blob_storage)

[Embulk](http://www.embulk.org/) file output plugin stores files on [Microsoft Azure](https://azure.microsoft.com/) [Blob Storage](https://azure.microsoft.com/en-us/documentation/articles/storage-introduction/#blob-storage)


## Overview

* **Plugin type**: file output
* **Resume supported**: no
* **Cleanup supported**: yes

## Configuration

First, create Azure [Storage Account](https://azure.microsoft.com/en-us/documentation/articles/storage-create-storage-account/).

- **account_name**: storage account name (string, required)
- **account_key**: primary access key (string, required)
- **container**: container name (string, required)
- **path_prefix**: prefix of target keys (string, required) (string, required)
- **file_ext**: e.g. "csv.gz, json.gz" (string, required)
- **blob_type**: `BLOCK_BLOB | PAGE_BLOB | APPEND_BLOB | UNSPECIFIED`, currently supports BLOCK_BLOB, other types will fall back into `UNSPECIFIED` (string, default `UNSPECIFIED`)


### Auto create container

container will create automatically when container doesn't exists.
 
When a container was deleted, a container with same name cannot be created for at least 30 seconds.
It's a [service specification](https://technet.microsoft.com/en-us/library/dd179408.aspx#Anchor_3) of Azure Blob Storage.

## Example

```yaml
out:
  type: azure_blob_storage
  account_name: myaccount
  account_key: myaccount_key
  container: my-container
  path_prefix: logs/csv-
  file_ext: csv.gz
  blob_type: BLOCK_BLOB
  formatter:
    type: csv
    header_line: false
  encoders:
  - {type: gzip}
```


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```


## Test

```
$ ./gradlew test  # -t to watch change of files and rebuild continuously
```

To run unit tests, we need to configure the following environment variables.

When environment variables are not set, skip some test cases.

```
AZURE_ACCOUNT_NAME
AZURE_ACCOUNT_KEY
AZURE_CONTAINER
AZURE_CONTAINER_DIRECTORY (optional, if needed)
```

If you're using Mac OS X El Capitan and GUI Applications(IDE), like as follows.
```xml
$ vi ~/Library/LaunchAgents/environment.plist
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>my.startup</string>
  <key>ProgramArguments</key>
  <array>
    <string>sh</string>
    <string>-c</string>
    <string>
      launchctl setenv AZURE_ACCOUNT_NAME my-account-name
      launchctl setenv AZURE_ACCOUNT_KEY my-account-key
      launchctl setenv AZURE_CONTAINER my-container
      launchctl setenv AZURE_CONTAINER_DIRECTORY unittests
    </string>
  </array>
  <key>RunAtLoad</key>
  <true/>
</dict>
</plist>

$ launchctl load ~/Library/LaunchAgents/environment.plist
$ launchctl getenv AZURE_ACCOUNT_NAME //try to get value.

Then start your applications.
```
