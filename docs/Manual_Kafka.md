



# Kafka

Module to produce, consume and administer Apache Kafka topics. Supports SASL/SSL authentication for Confluent Cloud, Azure Event Hubs, AWS MSK and self-hosted Kafka clusters.

*Read this in other languages: [English](Manual_Kafka.md), [Português](Manual_Kafka.pr.md), [Español](Manual_Kafka.es.md)*

![banner](imgs/Banner_Kafka.png o jpg)
## How to install this module

To install the module in Rocketbot Studio, it can be done in two ways:
1. Manual: __Download__ the .zip file and unzip it in the modules folder. The folder name must be the same as the module and inside it must have the following files and folders: \__init__.py, package.json, docs, example and libs. If you have the application open, refresh your browser to be able to use the new module.
2. Automatic: When entering Rocketbot Studio on the right margin you will find the **Addons** section, select **Install Mods**, search for the desired module and press install.

## How to use this module

Use this module to produce, consume and administer Apache Kafka topics from Rocketbot. It supports SASL/SSL authentication for Confluent Cloud, Azure Event Hubs, AWS MSK and self-hosted clusters.

Basic usage:
1. Run Connect Kafka with the bootstrap servers and, if required, the security protocol and SASL credentials. Optionally set a Session identifier to keep several connections open at once.
2. Run Test Connection to confirm Rocketbot can reach the cluster before building the rest of the flow.
3. Use the topic commands (List Topics, Create Topic, Delete Topic, Describe Topic, Get Offsets) to manage topics.
4. Use Produce Message / Produce Batch to publish, and Consume Messages / Commit Offsets / Close Consumer to read.

Sessions:
- Every command takes a Session identifier. Leave it empty to use the default connection, or set one to keep several independent connections/consumers open in the same flow (for example, one per cluster or per consumer group).
- 
Connect Kafka only stores the connection settings; it does not create the consumer yet.

Producing messages:
- Produce Message sends a single message (key optional; the value can be plain text or JSON).
- Produce Batch sends a JSON list of messages in one call, for example `[{"key": "1", "value": "hello"}, {"key": "2", "value": {"total": 100}}]`. Each item can be a plain value or a `{key, value}` object. Useful when the bot already has a list of records to publish together.

Consuming messages (consumer group flow):
1. Run Consume Messages with Topics (comma-separated), Group ID and Initial offset. These, together with Max poll interval, are only applied the first time the consumer is created for that session; to change them later, run Close Consumer first and call Consume Messages again.
2. Process the returned messages in the bot. Each message includes its own `topic`, `partition`, `offset`, `key` and `value`, so you can tell which topic it came from when subscribed to more than one.

3. Run Commit Offsets to tell Kafka the messages were processed. By default it commits the whole batch; if the bot only processed some of the messages, pass the successful ones in "Processed messages" so only those (and earlier ones) are committed, and the rest are redelivered on the next read.
4. Run Close Consumer when the flow finishes, or before changing Topics, Group ID, Initial offset or Max poll interval on the same session.

Important notes:
- Test Connection never stops the flow: it returns true or false so the bot can branch on the result. On failure, the reason is printed to the execution log, not returned in the result variable.
- Commit Offsets returns false, without raising an error, when there was nothing new to confirm since the last commit -- this is normal, not a failure. If the commit itself fails (for example, the consumer was evicted from the group), the command raises an error instead of returning false.
- Message values are stored and returned exactly as sent, 
including any whitespace, line breaks or formatting from the original text (Produce Message/Batch do not trim or reformat them, and Consume Messages returns them unchanged).
- Group ID, Initial offset and Max poll interval apply equally to every topic listed in a single Consume Messages call. To use different settings per topic, use a separate session for each one.
- If Commit Offsets runs later than the configured Max poll interval after the last Consume Messages call, Kafka may have already evicted the consumer from the group; run Consume Messages again or increase Max poll interval.
- Delete Topic is irreversible and, depending on the cluster configuration, also deletes the topic's messages. Double-check the topic name before running it in production.
- The first time the module runs on a machine, it installs the `confluent-kafka` client library automatically if a compatible version is not already bundled; this requires internet access on that first run.

References:
- 
https://kafka.apache.org/documentation/
- https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html


## Description of the commands

### Connect Kafka

Configures the connection to the Kafka cluster (bootstrap servers, security protocol, user, password). Can use an identifier to switch between multiple connections
|Parameters|Description|example|
| --- | --- | --- |
|Bootstrap servers|Comma-separated list of brokers (hostport)|broker1:9092,broker2:9092|
|Security protocol|Connection security protocol, default PLAINTEXT||
|SASL mechanism|SASL authentication mechanism, required if the protocol uses SASL||
|Username|SASL username (API key on Confluent Cloud)|username|
|Password|SASL password (API secret on Confluent Cloud)|secr3t_p@ss|
|Session|Connection identifier, if empty the default connection will be used|Conn1|
|Result|Variable where the result of the connection is stored|connected|

### Test Connection

Checks whether Rocketbot can connect to Kafka with the configured settings, returning true or false; on failure, the reason is printed to the execution log
|Parameters|Description|example|
| --- | --- | --- |
|Session|Identifier of the connection configured with 'Connect Kafka'|Conn1|
|Timeout (seconds)|Maximum time to wait for the test|5|
|Result|Variable with true if the connection succeeded, or false otherwise|connection_ok|

### List Topics

Lists the topics available in Kafka. A topic is like a channel where messages are published and read
|Parameters|Description|example|
| --- | --- | --- |
|Session|Connection identifier|Conn1|
|Timeout (seconds)|Maximum time to wait for the query|5|
|Result|Variable where the list of topics [{topic, partitions}] is stored|topics|

### Create Topic

Creates a new Kafka topic, indicating name, number of partitions and replication factor if applicable
|Parameters|Description|example|
| --- | --- | --- |
|Topic|Name of the new topic|my-topic|
|Partitions|Number of partitions for the topic, default 1|1|
|Replication factor|Replication factor for the topic, default 1|1|
|Session|Connection identifier|Conn1|
|Result|Variable where the creation result is stored|result|

### Delete Topic

IRREVERSIBLE: deletes a Kafka topic. Depending on the cluster configuration this also deletes its associated messages. Double-check the topic name before running this in production
|Parameters|Description|example|
| --- | --- | --- |
|Topic|Exact name of the topic to delete. This action cannot be undone|my-topic|
|Session|Connection identifier|Conn1|
|Result|Variable where the deletion result is stored|result|

### Describe Topic

Shows detailed information about a topic: partitions, leader, replicas, ISR, error state and configuration. Useful for diagnostics
|Parameters|Description|example|
| --- | --- | --- |
|Topic|Name of the topic to describe|my-topic|
|Timeout (seconds)|Maximum time to wait for the query, default 10|10|
|Session|Connection identifier|Conn1|
|Result|Variable with {topic, partitions [{partition, leader, replicas, isrs, error}], config {...}}|topic_detail|

### Get Offsets

Queries the earliest and latest available offset for each partition of a topic. Useful to know how many messages are available or from which point a consumer could read
|Parameters|Description|example|
| --- | --- | --- |
|Topic|Name of the topic to query|my-topic|
|Timeout (seconds)|Maximum time to wait for the query, default 10|10|
|Session|Connection identifier|Conn1|
|Result|Variable with list [{partition, earliest_offset, latest_offset, messages_available}]|offsets|

### Produce Message

Sends a single message to a topic. For example, publishing a JSON with invoice, order or event data
|Parameters|Description|example|
| --- | --- | --- |
|Topic|Name of the topic where the message will be published|my-topic|
|Key|Message key, determines the destination partition|key (optional)|
|Message|Content of the message to publish (string or JSON), sent exactly as typed, including line breaks|message content|
|Session|Connection identifier|Conn1|
|Result|Variable where the result of the send is stored|result|

### Produce Batch

Sends several messages together to a topic. Useful when the bot has a list of records to publish all at once
|Parameters|Description|example|
| --- | --- | --- |
|Topic|Name of the topic where the messages will be published|my-topic|
|Messages (JSON)|JSON list of messages. Each item can be {key, value} or a plain value|[{"key": "1", "value": "hello"}, {"key": "2", "value": {"total": 100}}]|
|Session|Connection identifier|Conn1|
|Result|Variable with {sent, failed} with the sent count and errors|result|

### Consume Messages

Reads messages from one or more topics, each tagged with its own 'topic' field. Limited by message count and timeout. The consumer (topics, group ID, initial offset, max poll interval) is created on the session's first call and reused afterwards; run 'Close Consumer' first to change those settings
|Parameters|Description|example|
| --- | --- | --- |
|Topics|Topics to consume, comma-separated. If you list more than one, each message can come from any of them; the 'topic' field tells you which one|topic1,topic2|
|Group ID|Consumer group identifier, shared by all topics in 'Topics'. Only applies when the consumer is created|my-group|
|Initial offset|Where to start reading if there is no committed offset. Only applies when the consumer is created||
|Timeout (seconds)|Maximum time to wait for a message before continuing|5|
|Max messages|Maximum number of messages to fetch in this run|10|
|Max poll interval (seconds)|Maximum time between calls to 'Consume Messages' or 'Commit Offsets' before Kafka evicts the consumer for inactivity. Default 1800 (30 min). Only applies when the consumer is created|1800|
|Session|Connection identifier|Conn1|
|Result|Variable where the list of received messages is stored. Each message includes topic, partition, offset, key and value, so you can tell which topic it came from when consuming several at once|messages|

### Commit Offsets

Confirms that consumed messages were processed and tells Kafka where to continue the next read. By default commits the whole batch from 'Consume Messages'; use 'Processed messages' to commit only up to the last successful one instead
|Parameters|Description|example|
| --- | --- | --- |
|Session|Connection identifier|Conn1|
|Processed messages (optional)|Optional. JSON list with the messages the bot processed successfully (same format returned by 'Consume Messages'). If left empty, the whole batch read is committed|[{"topic": "facturas", "partition": 0, "offset": 41}]|
|Result|Variable with true if committed, or false if there was nothing new to confirm|result|

### Close Consumer

Closes the consumer session and frees its resources. Required before changing consumer settings (Group ID, Initial offset, Max poll interval) on the next 'Consume Messages' call
|Parameters|Description|example|
| --- | --- | --- |
|Session|Connection identifier|Conn1|
