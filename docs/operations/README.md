# Orbit Events Kafka: operations and security

This guide organizes runtime behavior documented by the package. It does not certify production readiness. Verify provider/client versions, permissions, transport security, limits, and failure behavior in the target environment before release.

## Configuration surface

Environment names found in the package README:

The package README does not name `ORBIT_*` variables. Use its typed constructors and application configuration, and confirm exact runtime inputs in the implementation before deployment.

Use the package README's constructor and deployment examples. Store credentials in a secret manager and avoid logging credentials, raw provider errors, request data, or opaque cursors.

## Lifecycle, failure behavior, and limits

`orbit-events-kafka` is an optional Apache Kafka transport for Orbit Core's typed `EventTransport`
contract. It is installed independently; neither Core nor the in-process Core event bus gains an
`aiokafka` dependency.

```bash
python -m pip install orbit-core orbit-events-kafka
```

## Configure and publish

Keep broker credentials out of source control. The config masks SASL passwords in representations;
use TLS/SASL settings appropriate to the deployment and restrict Kafka ACLs to the required topics.

```python
from orbit import Application, ApplicationConfig
from orbit_events_kafka import KafkaConfig, KafkaEventTransport, KafkaTransportPlugin

application = Application(ApplicationConfig(name="orders"))
transport = KafkaEventTransport(
    KafkaConfig(
        bootstrap_servers=("localhost:9092",),
        topic_prefix="orbit.events",
        group_id="orders-service",
    )
)
application.plugins.register(KafkaTransportPlugin(transport))
await application.startup()

await transport.publish(order_created_event)
```

The plugin registers the transport under `KAFKA_EVENT_TRANSPORT_KEY` and closes it during Core
shutdown. Producer connection is lazy and occurs on the first publish. `subscribe()` starts a
consumer for one event name; a consumer group distributes that event's records among its members.
Register at most one handler for a given event name on a transport instance. Concurrent
`unsubscribe()` and transport shutdown are serialized so shutdown waits for a consumer stop already
in progress.

Transport shutdown rejects new work, cancels and joins consumer tasks, stops each consumer, then
flushes and stops the producer. Successfully stopped clients are released independently. If a client
stop fails or the caller is cancelled, the remaining client references stay owned; a later
`close()` retries cleanup. Configure the host's graceful-shutdown deadline to cover in-flight
handlers and the broker client's own request/close timeouts.

## Delivery contract and limits

Events use CloudEvents 1.0 structured JSON, advertise `application/cloudevents+json`, and are mapped
to `<topic_prefix>.<event_name>`. The
maximum encoded message size is configurable and bounded. Consumers disable automatic offset commits
and commit only after the handler completes, providing at-least-once processing. A failed handler is
retried a configured finite number of times with bounded backoff. Once retries are exhausted, that
consumer stops without committing the failed offset; restarting/rebalancing the group can deliver it
again. Inspect `failed_subscriptions` and application logs for stopped consumers.

This is not exactly-once processing: handlers must be idempotent when duplicate delivery matters.
To enable dead-lettering, provision a separate topic and configure
`KafkaConfig(dead_letter_topic="orbit.events.dead-letter")`. The producer writes the raw failed
record and bounded metadata then awaits broker acknowledgement before source offset commit. A
successful DLQ write followed by failed offset commit can yield duplicates; use the key's source
topic/partition/offset to recognize them. The feature does not provide an atomic transaction.

Restrict DLQ read access because original payloads can contain sensitive data. Configure retention,
replication, monitoring, and topic ACLs for the environment. This adapter does not create topics.
No live Kafka broker test is claimed by the fake-client unit suite.

## Production validation

Validate startup/shutdown cleanup, timeout and cancellation behavior, concurrency and payload bounds where applicable, secret rotation and least-privilege access, data durability, backup/restore, and failover against the selected provider. Do not infer distributed or durable guarantees from an in-process API or fake-client tests.
