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
"""Kafka implementation of Orbit Core's asynchronous event-transport contract."""

from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import AsyncIterator, Callable
from contextlib import suppress
from dataclasses import dataclass
from typing import Any, Protocol
from uuid import UUID, uuid4

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import ConsumerRecord, TopicPartition
from orbit.events import Event, EventHandler, EventTransport
from orbit_events import decode_event, encode_cloudevent

from orbit_events_kafka.config import KafkaConfig

_LOG = logging.getLogger(__name__)
_EVENT_NAME = re.compile(r"[a-z][a-z0-9.-]{0,126}\Z")


class KafkaTransportError(Exception):
    """Sanitized transport failure; vendor exception details and message data are not copied."""

    def __init__(self, operation: str) -> None:
        """Identify the operation that failed without exposing broker or event contents."""
        self.operation = operation
        super().__init__(f"Kafka {operation} operation failed.")


class _Producer(Protocol):
    """Minimal producer interface used by the transport and deterministic tests."""

    async def start(self) -> None:
        """Start the Kafka producer and establish its broker connections."""
        ...

    async def send_and_wait(
        self,
        topic: str,
        value: bytes,
        *,
        key: bytes | None = None,
        headers: list[tuple[str, bytes]] | None = None,
    ) -> object:
        """Send a serialized event and wait for the broker acknowledgement."""
        ...

    async def stop(self) -> None:
        """Flush pending records and release producer-owned resources."""
        ...


class _Consumer(Protocol):
    """Minimal consumer interface needed for manual commit and controlled shutdown."""

    def __aiter__(self) -> AsyncIterator[ConsumerRecord]: ...

    async def start(self) -> None:
        """Join the configured consumer group and begin polling the broker."""
        ...

    async def commit(self) -> None:
        """Commit processed offsets after the subscribed event handler succeeds."""
        ...

    async def stop(self) -> None:
        """Leave the group and release consumer-owned resources."""
        ...

    def seek(self, partition: TopicPartition, offset: int) -> None:
        """Reset one assigned partition so an uncommitted event can be retried."""
        ...


@dataclass
class _Subscription:
    """Owned consumer and task for one event-name handler."""

    event_name: str
    consumer: _Consumer
    handler: EventHandler
    task: asyncio.Task[None]


ProducerFactory = Callable[..., _Producer]
ConsumerFactory = Callable[..., _Consumer]


class KafkaEventTransport(EventTransport):
    """Publish Core events and consume one event name with manual offset commits.

    Each event name uses a consumer group derived from the configured group ID and topic name.
    Replicas using the same configuration share work for that event; handlers must be idempotent
    because a successful handler can be redelivered if its offset commit fails.
    """

    def __init__(
        self,
        config: KafkaConfig,
        *,
        producer_factory: ProducerFactory | None = None,
        consumer_factory: ConsumerFactory | None = None,
    ) -> None:
        """Bind validated settings and optional client factories for isolated contract tests."""
        if not isinstance(config, KafkaConfig):
            raise TypeError("KafkaEventTransport requires a KafkaConfig instance.")
        self.config = config
        self._producer_factory = producer_factory or AIOKafkaProducer
        self._consumer_factory = consumer_factory or AIOKafkaConsumer
        self._producer: _Producer | None = None
        self._producer_lock = asyncio.Lock()
        self._publish_lock = asyncio.Lock()
        self._state_lock = asyncio.Lock()
        self._close_lock = asyncio.Lock()
        self._subscriptions: dict[UUID, _Subscription] = {}
        self._event_tokens: dict[str, UUID] = {}
        self._failed: set[UUID] = set()
        self._closed = False

    @property
    def failed_subscriptions(self) -> tuple[UUID, ...]:
        """Return IDs of consumers that exhausted retries or encountered a transport failure."""
        return tuple(sorted(self._failed, key=str))

    @property
    def closed(self) -> bool:
        """Whether shutdown has begun and new transport operations are rejected."""
        return self._closed

    def topic_for(self, event_name: str) -> str:
        """Return the Kafka topic for a valid Core event name."""
        if not isinstance(event_name, str) or _EVENT_NAME.fullmatch(event_name) is None:
            raise ValueError("Kafka event names must match Orbit's lowercase event-name contract.")
        return f"{self.config.topic_prefix}.{event_name}"

    async def publish(self, event: Event[Any]) -> None:
        """Publish a JSON event envelope after enforcing the configured encoded-size limit."""
        if not isinstance(event, Event):
            raise TypeError("KafkaEventTransport.publish requires an Orbit Event.")
        if self._closed:
            raise KafkaTransportError("publish")
        topic = self.topic_for(event.name)
        payload = encode_cloudevent(event, max_bytes=self.config.max_message_bytes)
        producer = await self._ensure_producer()
        async with self._publish_lock:
            if self._closed:
                raise KafkaTransportError("publish")
            try:
                await producer.send_and_wait(
                    topic,
                    payload,
                    headers=[("content-type", b"application/cloudevents+json")],
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                raise KafkaTransportError("publish") from None

    async def _ensure_producer(self) -> _Producer:
        """Create and start exactly one producer under concurrent first publications."""
        async with self._producer_lock:
            if self._closed:
                raise KafkaTransportError("publish")
            if self._producer is None:
                producer = self._producer_factory(**self.config.client_options())
                try:
                    await producer.start()
                except asyncio.CancelledError:
                    await producer.stop()
                    raise
                except Exception:
                    with suppress(Exception):
                        await producer.stop()
                    raise KafkaTransportError("connect") from None
                self._producer = producer
            return self._producer

    async def check_connection(self) -> None:
        """Establish the configured producer connection without publishing an event."""
        await self._ensure_producer()

    async def subscribe(self, event_name: str, handler: EventHandler) -> UUID:
        """Start one manually committed consumer for an event name and return its token."""
        topic = self.topic_for(event_name)
        if not callable(handler):
            raise TypeError("Kafka event handler must be callable.")
        async with self._state_lock:
            if self._closed:
                raise KafkaTransportError("subscribe")
            if event_name in self._event_tokens:
                raise ValueError(
                    "Only one handler per event name is allowed per transport instance."
                )
            consumer = self._consumer_factory(
                topic,
                **self.config.client_options(),
                group_id=f"{self.config.group_id}.{event_name}",
                enable_auto_commit=False,
                auto_offset_reset="earliest",
            )
            try:
                await consumer.start()
            except asyncio.CancelledError:
                await consumer.stop()
                raise
            except Exception:
                with suppress(Exception):
                    await consumer.stop()
                raise KafkaTransportError("subscribe") from None
            token = uuid4()
            task = asyncio.create_task(self._consume(token, event_name, consumer, handler))
            self._subscriptions[token] = _Subscription(event_name, consumer, handler, task)
            self._event_tokens[event_name] = token
            return token

    async def _consume(
        self, token: UUID, event_name: str, consumer: _Consumer, handler: EventHandler
    ) -> None:
        """Retry a record without committing it; commit only after successful handler completion."""
        try:
            async for message in consumer:
                try:
                    event = decode_event(message.value, max_bytes=self.config.max_message_bytes)
                    if event.name != event_name:
                        raise ValueError("Event envelope name does not match its Kafka topic.")
                except Exception:
                    self._failed.add(token)
                    _LOG.error(
                        "Kafka consumer stopped after invalid event for topic %s", message.topic
                    )
                    if self.config.dead_letter_topic is not None:
                        await self._dead_letter(consumer, message, event_name, "InvalidEventError")
                    return

                delivered = False
                for attempt in range(self.config.max_handler_retries + 1):
                    try:
                        await handler(event)
                        await consumer.commit()
                        delivered = True
                        break
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        if attempt >= self.config.max_handler_retries:
                            self._failed.add(token)
                            _LOG.error(
                                "Kafka handler stopped after retries (%s)", type(exc).__name__
                            )
                            if self.config.dead_letter_topic is not None:
                                await self._dead_letter(
                                    consumer, message, event_name, type(exc).__name__
                                )
                            return
                        consumer.seek(message.topic_partition, message.offset)
                        await asyncio.sleep(self.config.retry_backoff_seconds)
                if not delivered:
                    return
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._failed.add(token)
            _LOG.error("Kafka consumer stopped after transport error (%s)", type(exc).__name__)

    async def _dead_letter(
        self, consumer: _Consumer, message: ConsumerRecord, event_name: str, reason: str
    ) -> None:
        """Persist original poison bytes before committing the source offset."""
        topic = self.config.dead_letter_topic
        if topic is None:
            return
        raw_value = message.value
        headers = [
            ("orbit-dead-letter-source-topic", message.topic.encode("utf-8")),
            ("orbit-dead-letter-source-partition", str(message.partition).encode("ascii")),
            ("orbit-dead-letter-source-offset", str(message.offset).encode("ascii")),
            ("orbit-dead-letter-event-name", event_name.encode("ascii")),
            ("orbit-dead-letter-reason", reason[:64].encode("ascii", errors="ignore")),
            ("orbit-dead-letter-content-type", b"application/octet-stream"),
            ("orbit-dead-letter-null-value", b"true" if raw_value is None else b"false"),
        ]
        producer = await self._ensure_producer()
        try:
            async with self._publish_lock:
                if self._closed:
                    raise KafkaTransportError("dead-letter")
                await producer.send_and_wait(
                    topic,
                    raw_value or b"",
                    key=f"{message.topic}:{message.partition}:{message.offset}".encode("ascii"),
                    headers=headers,
                )
                await consumer.commit()
        except asyncio.CancelledError:
            raise
        except Exception:
            # The source offset intentionally remains uncommitted if the DLQ write or commit fails.
            raise KafkaTransportError("dead-letter") from None

    async def unsubscribe(self, token: object) -> None:
        """Cancel and stop one owned consumer, rejecting unknown subscription tokens."""
        if not isinstance(token, UUID):
            raise TypeError("Kafka subscription token must be a UUID.")
        async with self._close_lock:
            async with self._state_lock:
                subscription = self._subscriptions.get(token)
                if subscription is None:
                    raise ValueError("Unknown Kafka subscription token.")
            subscription.task.cancel()
            await asyncio.gather(subscription.task, return_exceptions=True)
            try:
                await subscription.consumer.stop()
            except asyncio.CancelledError:
                raise
            except Exception:
                # Keep the subscription registered so transport.close() can retry cleanup.
                raise KafkaTransportError("unsubscribe") from None
            async with self._state_lock:
                self._subscriptions.pop(token, None)
                self._event_tokens.pop(subscription.event_name, None)

    async def close(self) -> None:
        """Stop all consumers and the producer, retaining failed cleanup for retry.

        The transport becomes unavailable before cleanup begins. Each resource stays owned until
        its ``stop()`` succeeds, so cancellation or a vendor cleanup error does not make a later
        ``close()`` silently forget a live client.
        """
        async with self._close_lock:
            async with self._state_lock:
                self._closed = True
                subscriptions = tuple(self._subscriptions.items())
            for _token, subscription in subscriptions:
                subscription.task.cancel()
            if subscriptions:
                await asyncio.gather(
                    *(subscription.task for _token, subscription in subscriptions),
                    return_exceptions=True,
                )
            failures: list[Exception] = []
            for _token, subscription in subscriptions:
                try:
                    await subscription.consumer.stop()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    failures.append(exc)
                else:
                    # close_lock serializes unsubscribe/close, and _closed prevents new
                    # subscriptions, so these ownership maps cannot be repopulated here.
                    self._subscriptions.pop(_token, None)
                    self._event_tokens.pop(subscription.event_name, None)
            async with self._publish_lock:
                async with self._producer_lock:
                    producer = self._producer
                if producer is not None:
                    try:
                        await producer.stop()
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        failures.append(exc)
                    else:
                        async with self._producer_lock:
                            if self._producer is producer:
                                self._producer = None
            if failures:
                raise KafkaTransportError("close") from None


__all__ = ["KafkaEventTransport", "KafkaTransportError"]
