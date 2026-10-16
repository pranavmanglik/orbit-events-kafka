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
"""Validated and redacted connection settings for the optional Kafka adapter."""

from __future__ import annotations

import re
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator, model_validator

_TOPIC = re.compile(r"[A-Za-z0-9._-]{1,249}\Z")


class KafkaConfig(BaseModel):
    """Connection, topic, and bounded delivery settings for `KafkaEventTransport`."""

    model_config = ConfigDict(frozen=True, extra="forbid", str_strip_whitespace=True)

    bootstrap_servers: tuple[str, ...] = Field(min_length=1, max_length=64)
    topic_prefix: str = "orbit.events"
    dead_letter_topic: str | None = None
    group_id: str = Field(min_length=1, max_length=100)
    security_protocol: Literal["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"] = "PLAINTEXT"
    sasl_mechanism: Literal["PLAIN"] | None = None
    sasl_username: SecretStr | None = None
    sasl_password: SecretStr | None = None
    max_message_bytes: int = Field(default=1_048_576, ge=1, le=10_485_760)
    max_handler_retries: int = Field(default=3, ge=0, le=100)
    retry_backoff_seconds: float = Field(default=0.25, gt=0, le=60)

    @field_validator("bootstrap_servers")
    @classmethod
    def validate_bootstrap_servers(cls, servers: tuple[str, ...]) -> tuple[str, ...]:
        """Reject blank broker addresses before passing settings to the vendor client."""
        if any(
            not server or any(character.isspace() for character in server) for server in servers
        ):
            raise ValueError("Kafka bootstrap servers must be non-empty host:port values.")
        if len(set(servers)) != len(servers):
            raise ValueError("Kafka bootstrap servers must be unique.")
        return servers

    @field_validator("topic_prefix")
    @classmethod
    def validate_topic_prefix(cls, value: str) -> str:
        """Keep generated topics within Kafka's bounded portable name alphabet."""
        if not value or len(value) > 100 or value.startswith((".", "_", "-")):
            raise ValueError("Kafka topic prefix must be 1–100 portable characters.")
        if any(
            not (character.isascii() and (character.isalnum() or character in "._-"))
            for character in value
        ):
            raise ValueError(
                "Kafka topic prefix may contain only ASCII letters, digits, '.', '_' and '-'."
            )
        return value

    @field_validator("dead_letter_topic")
    @classmethod
    def validate_dead_letter_topic(cls, value: str | None) -> str | None:
        """Validate an explicitly configured Kafka DLQ topic name."""
        if value is None:
            return None
        if (
            _TOPIC.fullmatch(value) is None
            or value.startswith((".", "_", "-"))
            or value in {".", ".."}
        ):
            raise ValueError("Kafka dead-letter topic must use 1–249 portable topic characters.")
        return value

    @model_validator(mode="after")
    def validate_dead_letter_topic_is_distinct(self) -> KafkaConfig:
        """Reject a DLQ topic that collides with this package's generated event topics."""
        if self.dead_letter_topic is not None and self.dead_letter_topic.startswith(
            f"{self.topic_prefix}."
        ):
            raise ValueError("Kafka dead-letter topic must not overlap generated event topics.")
        return self

    @model_validator(mode="after")
    def validate_sasl(self) -> KafkaConfig:
        """Require complete PLAIN credentials only when a SASL protocol is selected."""
        uses_sasl = self.security_protocol.startswith("SASL_")
        credentials_present = self.sasl_username is not None or self.sasl_password is not None
        if uses_sasl and (
            self.sasl_mechanism != "PLAIN"
            or self.sasl_username is None
            or self.sasl_password is None
            or not self.sasl_username.get_secret_value()
            or not self.sasl_password.get_secret_value()
        ):
            raise ValueError("SASL protocols require PLAIN mechanism, username, and password.")
        if not uses_sasl and (credentials_present or self.sasl_mechanism is not None):
            raise ValueError("SASL credentials require a SASL security protocol.")
        return self

    def client_options(self) -> dict[str, object]:
        """Build vendor client options, unwrapping secrets only at the connection boundary."""
        options: dict[str, object] = {
            "bootstrap_servers": self.bootstrap_servers,
            "security_protocol": self.security_protocol,
            "client_id": "orbit-events-kafka",
        }
        if self.sasl_mechanism is not None:
            options["sasl_mechanism"] = self.sasl_mechanism
        if self.sasl_username is not None:
            options["sasl_plain_username"] = self.sasl_username.get_secret_value()
        if self.sasl_password is not None:
            options["sasl_plain_password"] = self.sasl_password.get_secret_value()
        return options


__all__ = ["KafkaConfig"]
