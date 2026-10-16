# Copyright 2026-present Orbit Contributors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Kafka transport tests use fake clients and never require a broker."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from types import SimpleNamespace
from typing import Any

import pytest
from orbit import Application, ApplicationConfig
from orbit.events import Event
from pydantic import SecretStr, ValidationError

from orbit_events_kafka import (
    KAFKA_EVENT_TRANSPORT_KEY,
    KafkaConfig,
    KafkaEventTransport,
    KafkaTransportError,
    KafkaTransportPlugin,
)


def make_config(**updates: Any) -> KafkaConfig:
    """Construct the shared fake-broker settings with test-specific overrides."""
    values: dict[str, Any] = {
        "bootstrap_servers": ("localhost:9092",),
        "group_id": "orbit-tests",
    }
    values.update(updates)
    return KafkaConfig(**values)


class FakeProducer:
    """Record producer lifecycle and sent messages for adapter contract tests."""

    def __init__(self, **options: object) -> None:
        """Capture options passed to the injected client factory."""
        self.options = options
        self.messages: list[tuple[str, bytes]] = []
        self.keys: list[bytes | None] = []
        self.sent_headers: list[list[tuple[str, bytes]] | None] = []
        self.started = False
        self.stopped = False
        self.failure: Exception | None = None

    async def start(self) -> None:
        """Mark the fake producer connected."""
        self.started = True

    async def send_and_wait(
        self,
        topic: str,
        value: bytes,
        *,
        key: bytes | None = None,
        headers: list[tuple[str, bytes]] | None = None,
    ) -> object:
        """Capture bytes or simulate a vendor-side send failure."""
        if self.failure is not None:
            raise self.failure
        self.messages.append((topic, value))
        self.keys.append(key)
        self.sent_headers.append(headers)
        self.headers = headers
        return object()

    async def stop(self) -> None:
        """Mark the fake producer closed."""
        self.stopped = True


class FakeConsumer:
    """Async fake consumer with seek support for handler retry tests."""

    def __init__(self, topic: str, **options: object) -> None:
        """Store its topic and group settings and initialize an empty record queue."""
        self.topic = topic
        self.options = options
        self.records: asyncio.Queue[Any] = asyncio.Queue()
        self.started = False
        self.stopped = False
        self.commits = 0
        self._current: Any = None

    def __aiter__(self) -> AsyncIterator[Any]:
        """Return this queue-backed consumer as its own asynchronous iterator."""
        return self

    async def __anext__(self) -> Any:
        """Wait for the next fake record until shutdown sends the iterator sentinel."""
        record = await self.records.get()
        if record is None:
            raise StopAsyncIteration
        self._current = record
        return record

    async def start(self) -> None:
        """Mark the fake group consumer active."""
        self.started = True

    async def commit(self) -> None:
        """Count offsets committed after handler success."""
        self.commits += 1

    async def stop(self) -> None:
        """Wake a blocked iterator and mark the fake consumer closed."""
        if not self.stopped:
            self.stopped = True
            self.records.put_nowait(None)

    def seek(self, partition: object, offset: int) -> None:
        """Requeue the single fake record after a failed handler attempt."""
        del partition, offset
        if self._current is not None:
            self.records.put_nowait(self._current)


def test_config_requires_safe_consistent_sasl_settings_and_redacts_password() -> None:
    """The config validates broker and auth combinations and never reprs credentials."""
    config = make_config(
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_username=SecretStr("orbit-client"),
        sasl_password=SecretStr("secret-value"),
    )
    assert "secret-value" not in repr(config)
    assert config.client_options()["sasl_plain_password"] == "secret-value"
    with pytest.raises(ValidationError):
        make_config(bootstrap_servers=("",))
    with pytest.raises(ValidationError):
        make_config(security_protocol="SASL_SSL")
    with pytest.raises(ValidationError):
        make_config(topic_prefix=".unsafe")


@pytest.mark.asyncio
async def test_publish_is_lazy_json_encoded_bounded_and_sanitizes_driver_errors() -> None:
    """Producer lifecycle is lazy, event topics are deterministic, and broker errors are safe."""
    producers: list[FakeProducer] = []

    def create_producer(**options: object) -> FakeProducer:
        producer = FakeProducer(**options)
        producers.append(producer)
        return producer

    transport = KafkaEventTransport(make_config(), producer_factory=create_producer)
    assert producers == []
    event = Event(name="orders.created", payload={"order_id": "o-1"})
    await asyncio.gather(*(transport.publish(event) for _ in range(8)))
    assert len(producers) == 1
    assert producers[0].started
    assert producers[0].messages[0][0] == "orbit.events.orders.created"
    assert len(producers[0].messages) == 8
    assert b'"specversion":"1.0"' in producers[0].messages[0][1]
    assert producers[0].headers == [("content-type", b"application/cloudevents+json")]
    await transport.close()
    assert producers[0].stopped
    with pytest.raises(KafkaTransportError):
        await transport.publish(event)

    oversized = KafkaEventTransport(
        make_config(max_message_bytes=20), producer_factory=create_producer
    )
    with pytest.raises(ValueError, match="byte limit"):
        await oversized.publish(event)
    assert len(producers) == 1

    failed_producer: FakeProducer | None = None

    def create_failed_producer(**options: object) -> FakeProducer:
        nonlocal failed_producer
        failed_producer = FakeProducer(**options)
        failed_producer.failure = RuntimeError("private broker credential detail")
        return failed_producer

    failed_transport = KafkaEventTransport(make_config(), producer_factory=create_failed_producer)
    with pytest.raises(KafkaTransportError) as failure:
        await failed_transport.publish(event)
    assert "private broker credential" not in str(failure.value)
    assert failure.value.__suppress_context__
    await failed_transport.close()


@pytest.mark.asyncio
async def test_subscriber_commits_only_after_handler_and_closes_with_transport() -> None:
    """Consumer options disable auto-commit and offsets advance after handler success."""
    consumers: list[FakeConsumer] = []

    def create_consumer(topic: str, **options: object) -> FakeConsumer:
        consumer = FakeConsumer(topic, **options)
        consumers.append(consumer)
        return consumer

    transport = KafkaEventTransport(make_config(), consumer_factory=create_consumer)
    handled = asyncio.Event()

    async def handler(event: Event[Any]) -> None:
        assert event.name == "orders.created"
        handled.set()

    token = await transport.subscribe("orders.created", handler)
    consumer = consumers[0]
    assert consumer.options["enable_auto_commit"] is False
    assert consumer.options["group_id"] == "orbit-tests.orders.created"
    assert consumer.started
    consumer.records.put_nowait(
        SimpleNamespace(
            value=Event(name="orders.created", payload={"order_id": "o-1"})
            .model_dump_json()
            .encode(),
            topic=consumer.topic,
            topic_partition=object(),
            offset=0,
        )
    )
    await asyncio.wait_for(handled.wait(), timeout=1)
    await asyncio.wait_for(_wait_for_commit(consumer), timeout=1)
    assert consumer.commits == 1
    await transport.unsubscribe(token)
    assert consumer.stopped
    await transport.close()


@pytest.mark.asyncio
async def test_transport_close_waits_for_an_unsubscribe_already_stopping_its_consumer() -> None:
    """Concurrent transport shutdown must not miss a consumer removed by unsubscribe."""
    stopping = asyncio.Event()
    finish_stop = asyncio.Event()
    consumers: list[FakeConsumer] = []

    class SlowStopConsumer(FakeConsumer):
        async def stop(self) -> None:
            stopping.set()
            await finish_stop.wait()
            await super().stop()

    def create_consumer(topic: str, **options: object) -> SlowStopConsumer:
        consumer = SlowStopConsumer(topic, **options)
        consumers.append(consumer)
        return consumer

    transport = KafkaEventTransport(make_config(), consumer_factory=create_consumer)
    token = await transport.subscribe("orders.created", _ignore_event)
    unsubscribe_task = asyncio.create_task(transport.unsubscribe(token))
    await stopping.wait()
    close_task = asyncio.create_task(transport.close())
    await asyncio.sleep(0)

    assert not close_task.done()
    finish_stop.set()
    await unsubscribe_task
    await close_task
    assert consumers[0].stopped


@pytest.mark.asyncio
async def test_failed_unsubscribe_stop_remains_owned_for_transport_close_retry() -> None:
    """A failed explicit stop leaves enough ownership for transport shutdown to retry it."""
    consumers: list[FakeConsumer] = []

    class FailOnceStopConsumer(FakeConsumer):
        def __init__(self, topic: str, **options: object) -> None:
            super().__init__(topic, **options)
            self.stop_attempts = 0

        async def stop(self) -> None:
            self.stop_attempts += 1
            if self.stop_attempts == 1:
                raise RuntimeError("private broker state")
            await super().stop()

    def create_consumer(topic: str, **options: object) -> FailOnceStopConsumer:
        consumer = FailOnceStopConsumer(topic, **options)
        consumers.append(consumer)
        return consumer

    transport = KafkaEventTransport(make_config(), consumer_factory=create_consumer)
    token = await transport.subscribe("orders.created", _ignore_event)
    with pytest.raises(KafkaTransportError, match="unsubscribe"):
        await transport.unsubscribe(token)

    await transport.close()
    assert consumers[0].stop_attempts == 2
    assert consumers[0].stopped


@pytest.mark.asyncio
async def test_close_retries_consumer_and_producer_resources_that_failed_to_stop() -> None:
    """Partial shutdown retains only clients whose vendor stop operation failed."""
    consumers: list[FakeConsumer] = []
    producers: list[FakeProducer] = []

    class FailOnceConsumer(FakeConsumer):
        def __init__(self, topic: str, **options: object) -> None:
            super().__init__(topic, **options)
            self.stop_attempts = 0

        async def stop(self) -> None:
            self.stop_attempts += 1
            if self.stop_attempts == 1:
                raise RuntimeError("private consumer state")
            await super().stop()

    class FailOnceProducer(FakeProducer):
        def __init__(self, **options: object) -> None:
            super().__init__(**options)
            self.stop_attempts = 0

        async def stop(self) -> None:
            self.stop_attempts += 1
            if self.stop_attempts == 1:
                raise RuntimeError("private producer state")
            await super().stop()

    def create_consumer(topic: str, **options: object) -> FailOnceConsumer:
        consumer = FailOnceConsumer(topic, **options)
        consumers.append(consumer)
        return consumer

    def create_producer(**options: object) -> FailOnceProducer:
        producer = FailOnceProducer(**options)
        producers.append(producer)
        return producer

    transport = KafkaEventTransport(
        make_config(),
        consumer_factory=create_consumer,
        producer_factory=create_producer,
    )
    await transport.subscribe("orders.created", _ignore_event)
    await transport.publish(Event(name="orders.created", payload={"order_id": "o-1"}))

    with pytest.raises(KafkaTransportError, match="close"):
        await transport.close()

    assert consumers[0].stop_attempts == 1
    assert producers[0].stop_attempts == 1
    await transport.close()
    assert consumers[0].stop_attempts == 2
    assert consumers[0].stopped
    assert producers[0].stop_attempts == 2
    assert producers[0].stopped


@pytest.mark.asyncio
async def test_cancelled_close_keeps_consumer_for_a_later_cleanup_attempt() -> None:
    """Caller cancellation does not erase ownership before consumer stop completes."""
    stopping = asyncio.Event()
    consumers: list[FakeConsumer] = []

    class CancelFirstStopConsumer(FakeConsumer):
        def __init__(self, topic: str, **options: object) -> None:
            super().__init__(topic, **options)
            self.stop_attempts = 0

        async def stop(self) -> None:
            self.stop_attempts += 1
            if self.stop_attempts == 1:
                stopping.set()
                await asyncio.Event().wait()
            await super().stop()

    def create_consumer(topic: str, **options: object) -> CancelFirstStopConsumer:
        consumer = CancelFirstStopConsumer(topic, **options)
        consumers.append(consumer)
        return consumer

    transport = KafkaEventTransport(make_config(), consumer_factory=create_consumer)
    await transport.subscribe("orders.created", _ignore_event)
    close_task = asyncio.create_task(transport.close())
    await stopping.wait()
    close_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await close_task

    await transport.close()
    assert consumers[0].stop_attempts == 2
    assert consumers[0].stopped


async def _ignore_event(event: Event[Any]) -> None:
    """No-op handler for lifecycle tests that do not publish records."""
    del event


async def _wait_for_commit(consumer: FakeConsumer) -> None:
    """Yield until the consumer's async handler has advanced its offset."""
    while consumer.commits == 0:
        await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_exhausted_handler_retries_are_visible_and_not_committed() -> None:
    """A poison event leaves its offset uncommitted and marks that consumer as failed."""
    consumers: list[FakeConsumer] = []

    def create_consumer(topic: str, **options: object) -> FakeConsumer:
        consumer = FakeConsumer(topic, **options)
        consumers.append(consumer)
        return consumer

    transport = KafkaEventTransport(
        make_config(max_handler_retries=1, retry_backoff_seconds=0.001),
        consumer_factory=create_consumer,
    )
    attempts = 0

    async def handler(event: Event[Any]) -> None:
        nonlocal attempts
        del event
        attempts += 1
        raise RuntimeError("sensitive handler failure")

    token = await transport.subscribe("orders.failed", handler)
    consumer = consumers[0]
    consumer.records.put_nowait(
        SimpleNamespace(
            value=Event(name="orders.failed", payload={}).model_dump_json().encode(),
            topic=consumer.topic,
            topic_partition=object(),
            offset=3,
        )
    )
    async with asyncio.timeout(1):
        while token not in transport.failed_subscriptions:
            await asyncio.sleep(0)
    assert attempts == 2
    assert consumer.commits == 0
    assert "sensitive handler failure" not in repr(transport.failed_subscriptions)
    await transport.close()
    assert consumer.stopped


@pytest.mark.asyncio
async def test_dead_letter_waits_for_broker_ack_before_committing_source_offset() -> None:
    """A configured DLQ receives original bytes and safe source metadata before commit."""
    consumers: list[FakeConsumer] = []
    producers: list[FakeProducer] = []

    def create_consumer(topic: str, **options: object) -> FakeConsumer:
        consumer = FakeConsumer(topic, **options)
        consumers.append(consumer)
        return consumer

    def create_producer(**options: object) -> FakeProducer:
        producer = FakeProducer(**options)
        producers.append(producer)
        return producer

    transport = KafkaEventTransport(
        make_config(max_handler_retries=0, dead_letter_topic="orbit.dead-letter"),
        consumer_factory=create_consumer,
        producer_factory=create_producer,
    )

    async def handler(_event: Event[Any]) -> None:
        raise RuntimeError("private application detail")

    token = await transport.subscribe("orders.failed", handler)
    consumer = consumers[0]
    raw = Event(name="orders.failed", payload={"sensitive": "body"}).model_dump_json().encode()
    consumer.records.put_nowait(
        SimpleNamespace(
            value=raw,
            topic=consumer.topic,
            partition=2,
            topic_partition=object(),
            offset=13,
        )
    )
    async with asyncio.timeout(1):
        while token not in transport.failed_subscriptions:
            await asyncio.sleep(0)

    assert consumer.commits == 1
    assert producers[0].messages == [("orbit.dead-letter", raw)]
    assert producers[0].keys == [b"orbit.events.orders.failed:2:13"]
    header_map = dict(producers[0].sent_headers[0] or [])
    assert header_map["orbit-dead-letter-reason"] == b"RuntimeError"
    assert header_map["orbit-dead-letter-source-offset"] == b"13"
    assert b"private application detail" not in repr(header_map).encode()
    await transport.close()


@pytest.mark.asyncio
async def test_dead_letter_publish_failure_does_not_commit_source_offset() -> None:
    """Broker failure leaves the poison record uncommitted for a later delivery."""
    consumers: list[FakeConsumer] = []
    producers: list[FakeProducer] = []

    def create_consumer(topic: str, **options: object) -> FakeConsumer:
        consumer = FakeConsumer(topic, **options)
        consumers.append(consumer)
        return consumer

    def create_producer(**options: object) -> FakeProducer:
        producer = FakeProducer(**options)
        producer.failure = RuntimeError("private broker detail")
        producers.append(producer)
        return producer

    transport = KafkaEventTransport(
        make_config(max_handler_retries=0, dead_letter_topic="orbit.dead-letter"),
        consumer_factory=create_consumer,
        producer_factory=create_producer,
    )

    async def handler(_event: Event[Any]) -> None:
        raise RuntimeError("handler failed")

    token = await transport.subscribe("orders.failed", handler)
    consumer = consumers[0]
    consumer.records.put_nowait(
        SimpleNamespace(
            value=Event(name="orders.failed", payload={}).model_dump_json().encode(),
            topic=consumer.topic,
            partition=0,
            topic_partition=object(),
            offset=1,
        )
    )
    async with asyncio.timeout(1):
        while token not in transport.failed_subscriptions:
            await asyncio.sleep(0)
    assert consumer.commits == 0
    await transport.close()


@pytest.mark.asyncio
async def test_invalid_event_bytes_are_retained_in_dead_letter_topic() -> None:
    """Malformed bytes bypass handlers and reach the configured DLQ before offset commit."""
    consumers: list[FakeConsumer] = []
    producers: list[FakeProducer] = []

    def create_consumer(topic: str, **options: object) -> FakeConsumer:
        consumer = FakeConsumer(topic, **options)
        consumers.append(consumer)
        return consumer

    def create_producer(**options: object) -> FakeProducer:
        producer = FakeProducer(**options)
        producers.append(producer)
        return producer

    transport = KafkaEventTransport(
        make_config(dead_letter_topic="orbit.dead-letter"),
        consumer_factory=create_consumer,
        producer_factory=create_producer,
    )
    token = await transport.subscribe("orders.invalid", _ignore_event)
    consumer = consumers[0]
    raw = b'{"specversion":"broken","private":"data"}'
    consumer.records.put_nowait(
        SimpleNamespace(
            value=raw,
            topic=consumer.topic,
            partition=0,
            topic_partition=object(),
            offset=9,
        )
    )
    async with asyncio.timeout(1):
        while token not in transport.failed_subscriptions:
            await asyncio.sleep(0)

    assert consumer.commits == 1
    assert producers[0].messages == [("orbit.dead-letter", raw)]
    headers = dict(producers[0].sent_headers[0] or [])
    assert headers["orbit-dead-letter-reason"] == b"InvalidEventError"
    await transport.close()


@pytest.mark.asyncio
async def test_core_plugin_registers_and_owns_transport_lifecycle() -> None:
    """The optional adapter is resolved through Core's container and closed at shutdown."""
    producers: list[FakeProducer] = []

    def create_producer(**options: object) -> FakeProducer:
        producer = FakeProducer(**options)
        producers.append(producer)
        return producer

    transport = KafkaEventTransport(make_config(), producer_factory=create_producer)
    app = Application(ApplicationConfig(name="kafka-plugin-test"))
    app.plugins.register(KafkaTransportPlugin(transport))
    await app.startup()
    assert await app.container.aresolve(KAFKA_EVENT_TRANSPORT_KEY) is transport
    await app.stop()
    assert transport.closed
    assert not producers


@pytest.mark.asyncio
async def test_core_plugin_connection_action_establishes_and_closes_producer() -> None:
    """The CLI connectivity action sends no records and leaves cleanup to the plugin."""
    producers: list[FakeProducer] = []

    def create_producer(**options: object) -> FakeProducer:
        producer = FakeProducer(**options)
        producers.append(producer)
        return producer

    transport = KafkaEventTransport(make_config(), producer_factory=create_producer)
    plugin = KafkaTransportPlugin(transport)

    result = await plugin.control_actions[0].handler({})

    assert result is not None and result.summary == "Kafka producer connection is established."
    assert len(producers) == 1 and producers[0].started and not producers[0].messages
    await plugin.deactivate()
    assert producers[0].stopped
