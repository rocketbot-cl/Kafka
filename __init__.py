# coding: utf-8
"""
Kafka module for Rocketbot.

Business logic lives in libs/KafkaObject.py; this file is only a thin
dispatcher (reads parameters with GetParams, calls a KafkaObject method,
stores the result with SetVar). 'confluent-kafka' is installed only the
first time the module runs: if the import fails because there is no valid
wheel for the current OS/architecture/Python version, 'pip install
confluent-kafka -t libs' is run automatically and the import is retried.

v1 commands (module):
    connect         -> opens a session with the cluster connection settings
    test_connection -> validates that the configured connection works (network/credentials)
    list_topics     -> lists the cluster's topics
    create_topic    -> creates a new topic
    delete_topic    -> deletes a topic (IRREVERSIBLE, also removes its messages)
    describe_topic  -> partitions, leader, replicas, ISR and config of a topic
    get_offsets     -> oldest and newest offset available per partition
    produce         -> publishes a message to a topic
    produce_batch   -> publishes several messages together to a topic
    consume         -> polls for pending messages on one or more topics
    commit          -> confirms the offsets consumed by the session
    close_consumer  -> closes the session's consumer and releases its resources

Pending v2 roadmap:
    Kafka Connect: list/create/status/pause/resume/restart/delete connectors
    (will need the 'requests' library to talk to the Kafka Connect REST
    endpoint, not confluent-kafka)
"""
import os
import sys

base_path = tmp_global_obj["basepath"]
module_path = os.path.join(base_path, 'modules', 'Kafka', 'libs')
if module_path not in sys.path:
    sys.path.append(module_path)

from KafkaObject import KafkaObject, KafkaPartialResultError

module = GetParams("module")

# Rocketbot executes this file with exec(), reusing the same globals
# dictionary between successive calls (only the locals are reset). The
# 'global' + try/except NameError is what allows reusing the same
# KafkaObject instance (and its open sessions/connections) from one call to
# the next instead of recreating it on every command.
global mod_Kafka
try:
    mod_Kafka
except NameError:
    mod_Kafka = KafkaObject()

if module == "connect":
    bootstrap_servers = GetParams("bootstrap_servers")
    security_protocol = GetParams("security_protocol")
    sasl_mechanism = GetParams("sasl_mechanism")
    username = GetParams("username")
    password = GetParams("password")
    session = GetParams("session")
    result_var = GetParams("result")

    try:
        response = mod_Kafka.connect_command(
            bootstrap_servers, security_protocol, sasl_mechanism, username, password, session
        )
        SetVar(result_var, response)
    except Exception as e:
        SetVar(result_var, False)
        PrintException()
        raise e

elif module == "test_connection":
    session = GetParams("session")
    timeout = GetParams("timeout")
    result_var = GetParams("result")

    try:
        response = mod_Kafka.test_connection_command(session, timeout)
        SetVar(result_var, response)
    except Exception as e:
        SetVar(result_var, False)
        PrintException()
        raise e

elif module == "list_topics":
    session = GetParams("session")
    timeout = GetParams("timeout")
    result_var = GetParams("result")

    try:
        response = mod_Kafka.list_topics_command(session, timeout)
        SetVar(result_var, response)
    except Exception as e:
        SetVar(result_var, False)
        PrintException()
        raise e

elif module == "create_topic":
    session = GetParams("session")
    topic = GetParams("topic")
    partitions = GetParams("partitions")
    replication_factor = GetParams("replication_factor")
    result_var = GetParams("result")

    try:
        response = mod_Kafka.create_topic_command(session, topic, partitions, replication_factor)
        SetVar(result_var, response)
    except Exception as e:
        SetVar(result_var, False)
        PrintException()
        raise e

elif module == "delete_topic":
    session = GetParams("session")
    topic = GetParams("topic")
    result_var = GetParams("result")

    try:
        response = mod_Kafka.delete_topic_command(session, topic)
        SetVar(result_var, response)
    except Exception as e:
        SetVar(result_var, False)
        PrintException()
        raise e

elif module == "describe_topic":
    session = GetParams("session")
    topic = GetParams("topic")
    timeout = GetParams("timeout")
    result_var = GetParams("result")

    try:
        response = mod_Kafka.describe_topic_command(session, topic, timeout)
        SetVar(result_var, response)
    except Exception as e:
        SetVar(result_var, False)
        PrintException()
        raise e

elif module == "get_offsets":
    session = GetParams("session")
    topic = GetParams("topic")
    timeout = GetParams("timeout")
    result_var = GetParams("result")

    try:
        response = mod_Kafka.get_offsets_command(session, topic, timeout)
        SetVar(result_var, response)
    except Exception as e:
        SetVar(result_var, False)
        PrintException()
        raise e

elif module == "produce":
    session = GetParams("session")
    topic = GetParams("topic")
    key = GetParams("key")
    value = GetParams("value")
    result_var = GetParams("result")

    try:
        response = mod_Kafka.produce_command(session, topic, key, value)
        SetVar(result_var, response)
    except Exception as e:
        SetVar(result_var, False)
        PrintException()
        raise e

elif module == "produce_batch":
    session = GetParams("session")
    topic = GetParams("topic")
    messages = GetParams("messages")
    result_var = GetParams("result")

    try:
        response = mod_Kafka.produce_batch_command(session, topic, messages)
        SetVar(result_var, response)
    except Exception as e:
        SetVar(result_var, False)
        PrintException()
        raise e

elif module == "consume":
    session = GetParams("session")
    topics = GetParams("topics")
    group_id = GetParams("group_id")
    auto_offset_reset = GetParams("auto_offset_reset")
    timeout = GetParams("timeout")
    max_messages = GetParams("max_messages")
    max_poll_interval = GetParams("max_poll_interval")
    result_var = GetParams("result")

    try:
        response = mod_Kafka.consume_command(
            session, topics, group_id, auto_offset_reset, timeout, max_messages, max_poll_interval
        )
        SetVar(result_var, response)
    except KafkaPartialResultError as e:
        # The messages already read (and already counted by Kafka as
        # delivered) are stored anyway, so they are not lost if a Kafka
        # error happened mid-batch.
        SetVar(result_var, e.partial_result)
        PrintException()
        raise
    except Exception as e:
        SetVar(result_var, False)
        PrintException()
        raise e

elif module == "commit":
    session = GetParams("session")
    messages_param = GetParams("messages")
    result_var = GetParams("result")

    try:
        response = mod_Kafka.commit_command(session, messages_param)
        SetVar(result_var, response)
    except Exception as e:
        SetVar(result_var, False)
        PrintException()
        raise e

elif module == "close_consumer":
    session = GetParams("session")
    mod_Kafka.close_consumer_command(session)

else:
    raise Exception("Module '%s' is not implemented." % module)