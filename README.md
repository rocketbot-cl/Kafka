



# Kafka

Module to produce, consume and administer Apache Kafka topics. Supports SASL/SSL authentication for Confluent Cloud, Azure Event Hubs, AWS MSK and self-hosted Kafka clusters.

*Read this in other languages: [English](README.md), [Português](README.pr.md), [Español](README.es.md)*

## How to install this module

To install the module in Rocketbot Studio, it can be done in two ways:
1. Manual: __Download__ the .zip file and unzip it in the modules folder. The folder name must be the same as the module and inside it must have the following files and folders: \__init__.py, package.json, docs, example and libs. If you have the application open, refresh your browser to be able to use the new module.
2. Automatic: When entering Rocketbot Studio on the right margin you will find the **Addons** section, select **Install Mods**, search for the desired module and press install.


## Overview


1. Connect Kafka
Configures the connection to the Kafka cluster (bootstrap servers, security protocol, user, password). Can use an identifier to switch between multiple connections

2. Test Connection
Checks whether Rocketbot can connect to Kafka with the configured settings, returning true or false; on failure, the reason is printed to the execution log

3. List Topics
Lists the topics available in Kafka. A topic is like a channel where messages are published and read

4. Create Topic
Creates a new Kafka topic, indicating name, number of partitions and replication factor if applicable

5. Delete Topic
IRREVERSIBLE: deletes a Kafka topic. Depending on the cluster configuration this also deletes its associated messages. Double-check the topic name before running this in production

6. Describe Topic
Shows detailed information about a topic: partitions, leader, replicas, ISR, error state and configuration. Useful for diagnostics

7. Get Offsets
Queries the earliest and latest available offset for each partition of a topic. Useful to know how many messages are available or from which point a consumer could read

8. Produce Message
Sends a single message to a topic. For example, publishing a JSON with invoice, order or event data

9. Produce Batch
Sends several messages together to a topic. Useful when the bot has a list of records to publish all at once

10. Consume Messages
Reads messages from one or more topics, each tagged with its own 'topic' field. Limited by message count and timeout. The consumer (topics, group ID, initial offset, max poll interval) is created on the session's first call and reused afterwards; run 'Close Consumer' first to change those settings

11. Commit Offsets
Confirms that consumed messages were processed and tells Kafka where to continue the next read. By default commits the whole batch from 'Consume Messages'; use 'Processed messages' to commit only up to the last successful one instead

12. Close Consumer
Closes the consumer session and frees its resources. Required before changing consumer settings (Group ID, Initial offset, Max poll interval) on the next 'Consume Messages' call




----
### OS

- windows
- mac
- linux
- docker

### Dependencies

### License

![MIT](https://camo.githubusercontent.com/107590fac8cbd65071396bb4d04040f76cde5bde/687474703a2f2f696d672e736869656c64732e696f2f3a6c6963656e73652d6d69742d626c75652e7376673f7374796c653d666c61742d737175617265)
[MIT](http://opensource.org/licenses/mit-license.ph)