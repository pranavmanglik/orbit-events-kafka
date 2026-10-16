# Orbit Events Kafka

`orbit-events-kafka` is an optional Apache Kafka transport for Orbit Core's typed `EventTransport`
contract. It is installed independently; neither Core nor the in-process Core event bus gains an
`aiokafka` dependency.

```bash
python -m pip install orbit-core orbit-events orbit-events-kafka
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
shutdown. Producer connection is lazy and occurs on the first publish. Orbit CLI adds a
read-only **Check Kafka connection** action that establishes the producer without publishing an
event. The connection remains owned by the plugin and closes during application shutdown.
`subscribe()` starts a
consumer for one event name; a consumer group distributes that event's records among its members.
Register at most one handler for a given event name on a transport instance. Concurrent
`unsubscribe()` and transport shutdown are serialized so shutdown waits for a consumer stop already
in progress.

## Delivery contract and limits

Events use CloudEvents 1.0 structured JSON and are mapped to `<topic_prefix>.<event_name>`. The
maximum encoded message size is configurable and bounded. Consumers disable automatic offset commits
and commit only after the handler completes, providing at-least-once processing. A failed handler is
retried a configured finite number of times with bounded backoff. Once retries are exhausted, that
consumer stops without committing the failed offset; restarting/rebalancing the group can deliver it
again. Inspect `failed_subscriptions` and application logs for stopped consumers.

This is not exactly-once processing: handlers must be idempotent when duplicate delivery matters.
Dead-letter delivery is opt-in with `KafkaConfig(dead_letter_topic="orbit.events.dead-letter")`.
After handler retries are exhausted or a payload is invalid, the adapter publishes the original
bytes with bounded source metadata and waits for the broker acknowledgement before committing the
source offset. If publishing or committing fails, the source offset remains uncommitted. A commit
failure after a successful DLQ write can create a duplicate DLQ record; the record key contains the
source topic, partition, and offset so downstream consumers can identify duplicates. This is
at-least-once behavior, not an atomic Kafka transaction.

Provision the DLQ topic, its partitions, retention, ACLs, and monitoring before enabling it. Give
the application least-privilege write access and restrict DLQ readers because the original payload
may contain sensitive data. The adapter does not create topics, add a schema registry or outbox, or
provide cross-service transactions. No live Kafka broker test is claimed by the fake-client unit
suite.

## Documentation

The package-specific guides cover [architecture](docs/architecture/overview.md), [operations and security](docs/operations/README.md), and [development](docs/development/README.md), with [security guidance](docs/security/overview.md). The [documentation index](docs/README.md) links to the full package overview and project policies.

## Development and status

```bash
python -m pip install -e '.[dev]'
pytest
ruff check .
mypy
```

This package is alpha, its API is not stable, and it supports Python 3.11 through 3.14.

The producer/consumer lifecycle and manual-offset-commit behavior follow the
[aiokafka producer](https://aiokafka.readthedocs.io/en/latest/producer.html) and
[consumer](https://aiokafka.readthedocs.io/en/latest/consumer.html) APIs.

Licensed under Apache-2.0.

