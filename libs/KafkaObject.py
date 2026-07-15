import functools
import json

from confluent_kafka import Producer, Consumer, KafkaException, KafkaError, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource, OffsetSpec

SESSION_DEFAULT = "default"

# Most common KafkaError codes seen while using this module, mapped to an
# actionable message for whoever designs the flow, instead of the raw
# technical message that librdkafka returns.
KAFKA_ERROR_HINTS = {
    KafkaError._TRANSPORT: "Could not establish a network connection to the cluster. Check 'bootstrap_servers', firewall/VPN, and that the cluster is up.",
    KafkaError._ALL_BROKERS_DOWN: "Could not reach any broker in the cluster. Check 'bootstrap_servers' and the cluster's status.",
    KafkaError._AUTHENTICATION: "Authentication failed. Check the username/password (or API key/secret) and the configured 'sasl_mechanism'.",
    KafkaError._TIMED_OUT: "The operation exceeded the configured timeout. Increase 'timeout' or check connectivity to the cluster.",
    KafkaError.TOPIC_AUTHORIZATION_FAILED: "The user does not have permissions on the requested topic. Check the cluster's ACLs/security policies.",
    KafkaError.GROUP_AUTHORIZATION_FAILED: "The user does not have permissions on the requested consumer group. Check the cluster's ACLs/security policies.",
    KafkaError.UNKNOWN_TOPIC_OR_PART: "The requested topic or partition does not exist.",
    KafkaError.TOPIC_ALREADY_EXISTS: "A topic with that name already exists.",
}


class KafkaCommandError(Exception):
    """
    Kafka command error enriched with an actionable hint when the underlying
    error is a recognized KafkaError (see KAFKA_ERROR_HINTS). The original
    KafkaException stays chained as __cause__.
    """

    def __init__(self, message, code=None, hint=None):
        super().__init__(message)
        self.code = code
        self.hint = hint


class KafkaPartialResultError(Exception):
    """
    Error from 'Consume Messages' when Kafka returns an error mid-batch.
    Carries the messages already read in 'partial_result' so the dispatcher
    can store them in the result variable before re-raising: those messages
    were already delivered by Kafka and should not be lost even if the
    command ends in an error.
    """

    def __init__(self, message, partial_result):
        super().__init__(message)
        self.partial_result = partial_result


def describe_kafka_exception(exc):
    """Builds a KafkaCommandError with a hint if the KafkaException is recognized, or None if it does not apply."""
    error = exc.args[0] if exc.args else None
    if error is None:
        return None
    hint = KAFKA_ERROR_HINTS.get(error.code())
    if not hint:
        return None
    return KafkaCommandError("%s (%s)" % (str(error), hint), code=error.code(), hint=hint)


def with_kafka_error_hints(fn):
    """Decorator: if the method lets a recognized KafkaException escape, replaces it with a hinted KafkaCommandError."""

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except KafkaException as exc:
            hinted = describe_kafka_exception(exc)
            if hinted:
                raise hinted from exc
            raise

    return wrapper


def encode_value(value):
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value).encode("utf-8")
    return str(value).encode("utf-8")


def require_str(value, field_name):
    text = str(value).strip() if value is not None else ""
    if not text:
        raise Exception("Field '%s' is required" % field_name)
    return text


class KafkaObject:
    """Facade with the Kafka logic used by the Rocketbot commands. Keeps the sessions (config/producer/consumer) in memory."""

    def __init__(self):
        self.sessions = {}

    def require_session(self, session):
        if session not in self.sessions:
            raise Exception(
                "There is no open connection for session '%s'. Run 'Connect Kafka' first." % session
            )
        return self.sessions[session]

    @staticmethod
    def build_config(bootstrap_servers, security_protocol, sasl_mechanism, username, password):
        config = {"bootstrap.servers": bootstrap_servers}
        if security_protocol:
            config["security.protocol"] = security_protocol
        if sasl_mechanism:
            config["sasl.mechanism"] = sasl_mechanism
        if username:
            config["sasl.username"] = username
        if password:
            config["sasl.password"] = password
        return config

    def connect_command(self, bootstrap_servers, security_protocol, sasl_mechanism, username, password, session):
        bootstrap_servers = require_str(bootstrap_servers, "Bootstrap servers")
        session = session or SESSION_DEFAULT

        config = self.build_config(bootstrap_servers, security_protocol, sasl_mechanism, username, password)
        self.sessions[session] = {"config": config, "producer": None, "consumer": None}
        return True

    def test_connection_command(self, session, timeout):
        session = session or SESSION_DEFAULT
        timeout = float(timeout) if timeout else 5.0

        try:
            conn = self.require_session(session)
            admin = AdminClient(conn["config"])
            admin.list_topics(timeout=timeout)
            return True
        except KafkaException as exc:
            # Does not raise: a "test connection" command must let the bot
            # branch on the result instead of halting the flow. The reason
            # is printed (with the same hints as the other commands) so it
            # is still visible for debugging.
            hinted = describe_kafka_exception(exc)
            print("[Kafka] Test Connection failed: %s" % (hinted or exc))
            return False
        except Exception as e:
            print("[Kafka] Test Connection failed: %s" % e)
            return False

    @with_kafka_error_hints
    def list_topics_command(self, session, timeout):
        session = session or SESSION_DEFAULT
        timeout = float(timeout) if timeout else 5.0

        conn = self.require_session(session)
        admin = AdminClient(conn["config"])
        metadata = admin.list_topics(timeout=timeout)

        return [
            {"topic": name, "partitions": len(t.partitions)}
            for name, t in metadata.topics.items()
            if not name.startswith("__")
        ]

    @with_kafka_error_hints
    def create_topic_command(self, session, topic, partitions, replication_factor):
        topic = require_str(topic, "Topic")
        session = session or SESSION_DEFAULT
        partitions = int(partitions) if partitions else 1
        replication_factor = int(replication_factor) if replication_factor else 1

        conn = self.require_session(session)
        admin = AdminClient(conn["config"])
        new_topic = NewTopic(topic, num_partitions=partitions, replication_factor=replication_factor)
        futures = admin.create_topics([new_topic])
        futures[topic].result(10)
        return True

    @with_kafka_error_hints
    def delete_topic_command(self, session, topic):
        # WARNING: irreversible operation. Deletes the topic and, depending on
        # the cluster configuration, its associated messages become
        # unreachable/get removed.
        topic = require_str(topic, "Topic")
        session = session or SESSION_DEFAULT

        conn = self.require_session(session)
        admin = AdminClient(conn["config"])
        futures = admin.delete_topics([topic])
        futures[topic].result(15)
        return True

    @with_kafka_error_hints
    def describe_topic_command(self, session, topic, timeout):
        topic = require_str(topic, "Topic")
        session = session or SESSION_DEFAULT
        timeout = float(timeout) if timeout else 10.0

        conn = self.require_session(session)
        admin = AdminClient(conn["config"])

        metadata = admin.list_topics(topic=topic, timeout=timeout)
        if topic not in metadata.topics:
            raise Exception("Topic '%s' does not exist" % topic)

        topic_metadata = metadata.topics[topic]
        if topic_metadata.error is not None:
            raise KafkaException(topic_metadata.error)

        partitions = [
            {
                "partition": p.id,
                "leader": p.leader,
                "replicas": list(p.replicas),
                "isrs": list(p.isrs),
                "error": str(p.error) if p.error else None
            }
            for p in sorted(topic_metadata.partitions.values(), key=lambda part: part.id)
        ]

        config_resource = ConfigResource(ConfigResource.Type.TOPIC, topic)
        config_future = admin.describe_configs([config_resource], request_timeout=timeout)[config_resource]
        config_entries = config_future.result(timeout)
        config = {name: entry.value for name, entry in config_entries.items()}

        return {"topic": topic, "partitions": partitions, "config": config}

    @with_kafka_error_hints
    def get_offsets_command(self, session, topic, timeout):
        topic = require_str(topic, "Topic")
        session = session or SESSION_DEFAULT
        timeout = float(timeout) if timeout else 10.0

        conn = self.require_session(session)
        admin = AdminClient(conn["config"])

        metadata = admin.list_topics(topic=topic, timeout=timeout)
        if topic not in metadata.topics:
            raise Exception("Topic '%s' does not exist" % topic)

        partition_ids = sorted(metadata.topics[topic].partitions.keys())

        earliest_request = {TopicPartition(topic, p): OffsetSpec.earliest() for p in partition_ids}
        earliest_futures = admin.list_offsets(earliest_request, request_timeout=timeout)

        latest_request = {TopicPartition(topic, p): OffsetSpec.latest() for p in partition_ids}
        latest_futures = admin.list_offsets(latest_request, request_timeout=timeout)

        # Results come indexed by the same TopicPartition objects sent in the
        # request, not by the other call's objects, so two dicts keyed by
        # partition number are built before merging them.
        earliest_offsets = {tp.partition: f.result(timeout).offset for tp, f in earliest_futures.items()}
        latest_offsets = {tp.partition: f.result(timeout).offset for tp, f in latest_futures.items()}

        return [
            {
                "partition": p,
                "earliest_offset": earliest_offsets[p],
                "latest_offset": latest_offsets[p],
                "messages_available": latest_offsets[p] - earliest_offsets[p]
            }
            for p in partition_ids
        ]

    @with_kafka_error_hints
    def produce_command(self, session, topic, key, value):
        topic = require_str(topic, "Topic")
        value = require_str(value, "Message")
        session = session or SESSION_DEFAULT

        conn = self.require_session(session)
        if conn["producer"] is None:
            conn["producer"] = Producer(conn["config"])
        producer = conn["producer"]

        delivery_error = {}

        def delivery_report(err, msg):
            if err is not None:
                delivery_error["error"] = err

        producer.produce(
            topic,
            key=key.encode("utf-8") if key else None,
            value=value.encode("utf-8"),
            callback=delivery_report
        )
        producer.flush(10)

        if "error" in delivery_error:
            raise KafkaException(delivery_error["error"])

        return True

    @with_kafka_error_hints
    def produce_batch_command(self, session, topic, messages):
        topic = require_str(topic, "Topic")
        if messages in (None, ""):
            raise Exception("Field 'Messages (JSON)' is required")
        session = session or SESSION_DEFAULT

        conn = self.require_session(session)
        if conn["producer"] is None:
            conn["producer"] = Producer(conn["config"])
        producer = conn["producer"]

        items = json.loads(messages) if isinstance(messages, str) else messages
        if not isinstance(items, list):
            raise Exception("Field 'Messages (JSON)' must be a JSON list of messages")

        errors = []

        def delivery_report(err, msg):
            if err is not None:
                errors.append(str(err))

        for item in items:
            if isinstance(item, dict) and ("key" in item or "value" in item):
                key = item.get("key")
                value = item.get("value")
            else:
                key = None
                value = item

            producer.produce(
                topic,
                key=encode_value(key),
                value=encode_value(value),
                callback=delivery_report
            )

        producer.flush(30)

        return {"sent": len(items) - len(errors), "failed": errors}

    def consume_command(self, session, topics, group_id, auto_offset_reset, timeout, max_messages, max_poll_interval):
        topics = require_str(topics, "Topics")
        session = session or SESSION_DEFAULT

        conn = self.require_session(session)

        if conn["consumer"] is None:
            # max.poll.interval.ms: maximum time that can pass between two
            # poll() calls before Kafka evicts the consumer from the group
            # due to inactivity. In an RPA flow a fair amount of time can
            # pass between 'Consume Messages' and the next step (processing,
            # 'Commit Offsets', etc), so Kafka's default (5 min) is too
            # short. Default here: 30 minutes.
            consumer_config = dict(conn["config"])
            consumer_config["group.id"] = group_id or "rocketbot-kafka"
            consumer_config["auto.offset.reset"] = auto_offset_reset or "latest"
            consumer_config["enable.auto.commit"] = False
            consumer_config["max.poll.interval.ms"] = int(float(max_poll_interval) * 1000) if max_poll_interval else 1800000

            consumer = Consumer(consumer_config)
            topic_list = [t.strip() for t in topics.split(",") if t.strip()]
            consumer.subscribe(topic_list)
            conn["consumer"] = consumer

        consumer = conn["consumer"]

        timeout = float(timeout) if timeout else 5.0
        max_messages = int(max_messages) if max_messages else 10

        messages = []
        consume_error = None
        try:
            for _ in range(max_messages):
                msg = consumer.poll(timeout)
                if msg is None:
                    break
                if msg.error():
                    consume_error = str(msg.error())
                    break
                messages.append({
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                    "key": msg.key().decode("utf-8") if msg.key() else None,
                    "value": msg.value().decode("utf-8") if msg.value() else None
                })
        except KafkaException as exc:
            # The messages already read (and already counted by Kafka as
            # delivered) are preserved so the dispatcher can store them in
            # the result variable even though the command ends in an error.
            hinted = describe_kafka_exception(exc)
            message = str(hinted) if hinted else str(exc)
            raise KafkaPartialResultError(message, messages) from exc

        if consume_error:
            raise KafkaPartialResultError(
                "Read %d message(s) before a Kafka error: %s" % (len(messages), consume_error),
                messages
            )

        return messages

    def commit_command(self, session, messages_param):
        session = session or SESSION_DEFAULT

        conn = self.require_session(session)
        if conn["consumer"] is None:
            raise Exception(
                "There is no active consumer for session '%s'. Run 'Consume Messages' first." % session
            )

        consumer = conn["consumer"]

        try:
            if messages_param:
                # Partial commit: only confirms up to the highest offset per
                # partition present in 'messages' (for example, the messages
                # of the batch that the bot processed successfully before
                # hitting an error). The committed offset is "offset+1"
                # because in Kafka the committed offset means "the next
                # message to read", not "the last one read".
                items = json.loads(messages_param) if isinstance(messages_param, str) else messages_param
                if not isinstance(items, list):
                    items = [items]

                highest_offset = {}
                for item in items:
                    key = (item["topic"], item["partition"])
                    offset = item["offset"]
                    if key not in highest_offset or offset > highest_offset[key]:
                        highest_offset[key] = offset

                offsets_to_commit = [
                    TopicPartition(tp_topic, tp_partition, offset + 1)
                    for (tp_topic, tp_partition), offset in highest_offset.items()
                ]
                consumer.commit(offsets=offsets_to_commit, asynchronous=False)
            else:
                consumer.commit(asynchronous=False)

            return True
        except KafkaException as e:
            error = e.args[0] if e.args else None
            if error is not None and error.code() == KafkaError._NO_OFFSET:
                # No new message was consumed since the consumer was created
                # or since the last commit, so there is nothing to confirm.
                # This is normal (e.g. 'Consume Messages' brought nothing
                # back because the topic had no new messages) and should not
                # stop the bot's flow.
                return False
            if error is not None and error.code() == KafkaError._ASSIGNMENT_LOST:
                raise Exception(
                    "The commit failed because the consumer lost its group membership due to inactivity "
                    "(more time passed than allowed by 'max_poll_interval' between calls). "
                    "Increase the 'Max poll interval' parameter in 'Consume Messages' or call "
                    "'Consume Messages' again before retrying the commit."
                ) from e
            hinted = describe_kafka_exception(e)
            raise (hinted or e) from e

    def close_consumer_command(self, session):
        session = session or SESSION_DEFAULT
        conn = self.require_session(session)
        if conn["consumer"] is not None:
            conn["consumer"].close()
            conn["consumer"] = None
        return True