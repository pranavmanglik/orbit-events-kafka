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
"""Optional Core plugin that registers and owns one Kafka event transport."""

from collections.abc import Mapping

from orbit import Application
from orbit.plugins import ControlAction, ControlResult, Plugin, PluginMetadata

from orbit_events_kafka.transport import KafkaEventTransport

KAFKA_EVENT_TRANSPORT_KEY = "orbit.events.transport.kafka"


class KafkaTransportPlugin(Plugin):
    """Register a configured transport and close its clients during Core shutdown."""

    metadata = PluginMetadata(
        name="orbit-events-kafka",
        version="0.1.0a1",
        capabilities=frozenset({"events.kafka"}),
    )

    def __init__(self, transport: KafkaEventTransport) -> None:
        """Take lifecycle ownership of a transport configured by the application."""
        if not isinstance(transport, KafkaEventTransport):
            raise TypeError("KafkaTransportPlugin requires a KafkaEventTransport instance.")
        self.transport = transport
        self.control_actions = (
            ControlAction(
                id="check-connection",
                title="Check Kafka connection",
                description="Establish the producer connection without publishing an event.",
                handler=self._check_connection,
            ),
        )

    def setup(self, application: Application) -> None:
        """Expose the transport under its stable optional-package dependency key."""
        if not isinstance(application, Application):
            raise TypeError("KafkaTransportPlugin requires an Orbit Application.")
        application.container.register_instance(KAFKA_EVENT_TRANSPORT_KEY, self.transport)

    async def deactivate(self) -> None:
        """Stop consumers and producer on normal shutdown or startup rollback."""
        await self.transport.close()

    async def _check_connection(self, _: Mapping[str, object]) -> ControlResult:
        """Establish the bounded producer connection and retain it for normal plugin cleanup."""
        await self.transport.check_connection()
        return ControlResult("Kafka producer connection is established.")


__all__ = ["KAFKA_EVENT_TRANSPORT_KEY", "KafkaTransportPlugin"]
