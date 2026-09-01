#!/usr/bin/env python3
"""Strict MinIO helpers shared by native DuckDB and Vane Vortex tests."""

from __future__ import annotations

import json
import os
import subprocess
import urllib.parse
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path


def require_equal(actual: object, expected: object, description: str) -> None:
    if actual != expected:
        raise AssertionError(f"{description}: expected {expected!r}, got {actual!r}")


def require_true(value: bool, description: str) -> None:
    if not value:
        raise AssertionError(description)


def sql_string(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


@dataclass(frozen=True)
class MinioConfig:
    endpoint: str
    access_key: str
    secret_key: str
    region: str
    bucket: str

    @classmethod
    def from_env(cls) -> "MinioConfig":
        values = {
            name: os.environ.get(name, "").strip()
            for name in (
                "VORTEX_MINIO_ENDPOINT",
                "VORTEX_MINIO_ACCESS_KEY",
                "VORTEX_MINIO_SECRET_KEY",
                "VORTEX_MINIO_REGION",
                "VORTEX_MINIO_BUCKET",
            )
        }
        missing = sorted(name for name, value in values.items() if not value)
        if missing:
            raise RuntimeError(
                "object-store qualification requires: " + ", ".join(missing)
            )
        endpoint = values["VORTEX_MINIO_ENDPOINT"]
        parsed = urllib.parse.urlsplit(endpoint)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path not in {"", "/"}
            or parsed.query
            or parsed.fragment
        ):
            raise RuntimeError("VORTEX_MINIO_ENDPOINT must be an origin URL")
        bucket = values["VORTEX_MINIO_BUCKET"]
        if not bucket.isascii() or not all(
            character.islower() or character.isdigit() or character in ".-"
            for character in bucket
        ):
            raise RuntimeError("VORTEX_MINIO_BUCKET is not a canonical S3 bucket name")
        return cls(
            endpoint=endpoint.rstrip("/"),
            access_key=values["VORTEX_MINIO_ACCESS_KEY"],
            secret_key=values["VORTEX_MINIO_SECRET_KEY"],
            region=values["VORTEX_MINIO_REGION"],
            bucket=bucket,
        )

    @property
    def duckdb_endpoint(self) -> str:
        parsed = urllib.parse.urlsplit(self.endpoint)
        return parsed.netloc

    @property
    def use_ssl(self) -> bool:
        return urllib.parse.urlsplit(self.endpoint).scheme == "https"

    def uri(self, key: str) -> str:
        normalized = key.lstrip("/")
        require_true(bool(normalized), "an S3 object key must not be empty")
        return f"s3://{self.bucket}/{normalized}"

    def _redact(self, value: str) -> str:
        redacted = value.replace(self.access_key, "<redacted-access-key>")
        return redacted.replace(self.secret_key, "<redacted-secret-key>")

    def _object_url(self, key: str, query: str = "") -> str:
        encoded_bucket = urllib.parse.quote(self.bucket, safe="")
        encoded_key = urllib.parse.quote(key.lstrip("/"), safe="/")
        suffix = f"/{encoded_bucket}/{encoded_key}"
        if query:
            suffix += "?" + query
        return self.endpoint + suffix

    def _curl(self, *arguments: str, output: Path | None = None) -> bytes:
        command = [
            "curl",
            "--fail-with-body",
            "--silent",
            "--show-error",
            "--connect-timeout",
            "5",
            "--max-time",
            "60",
            "--aws-sigv4",
            f"aws:amz:{self.region}:s3",
            "--user",
            f"{self.access_key}:{self.secret_key}",
        ]
        if output is not None:
            command.extend(("--output", str(output)))
        command.extend(arguments)
        completed = subprocess.run(command, check=False, capture_output=True)
        if completed.returncode != 0:
            if output is not None:
                output.unlink(missing_ok=True)
            detail = (completed.stderr + completed.stdout).decode(
                "utf-8", errors="replace"
            )
            raise RuntimeError(
                f"signed S3 request failed with curl exit {completed.returncode}: "
                + self._redact(detail).strip()
            )
        return completed.stdout

    def download(self, uri: str, destination: Path) -> None:
        bucket, key = parse_s3_uri(uri)
        require_equal(bucket, self.bucket, "download bucket")
        destination.parent.mkdir(parents=True, exist_ok=True)
        self._curl(self._object_url(key), output=destination)
        require_true(
            destination.is_file() and destination.stat().st_size > 0,
            f"downloaded S3 object is empty: {uri}",
        )

    def read_bytes(self, uri: str) -> bytes:
        bucket, key = parse_s3_uri(uri)
        require_equal(bucket, self.bucket, "read bucket")
        return self._curl(self._object_url(key))

    def list_keys(self, prefix: str) -> list[str]:
        query = urllib.parse.urlencode(
            {"list-type": "2", "prefix": prefix.lstrip("/")}
        )
        payload = self._curl(self._object_url("", query))
        root = ET.fromstring(payload)
        keys = sorted(
            element.text or ""
            for element in root.findall("{*}Contents/{*}Key")
            if element.text
        )
        truncated = root.findtext("{*}IsTruncated", default="false").lower()
        require_equal(truncated, "false", "MinIO object listing truncation")
        return keys


def parse_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urllib.parse.urlsplit(str(uri))
    if (
        parsed.scheme != "s3"
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise AssertionError(f"not a canonical S3 URI: {uri!r}")
    key = urllib.parse.unquote(parsed.path.lstrip("/"))
    require_true(bool(key), f"S3 URI has no object key: {uri!r}")
    return parsed.netloc, key


def configure_s3_settings(
    connection: object,
    config: MinioConfig,
    *,
    endpoint: str | None = None,
) -> None:
    selected_endpoint = endpoint or config.endpoint
    parsed = urllib.parse.urlsplit(selected_endpoint)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise AssertionError("S3 test endpoint must be an HTTP origin")
    statements = (
        "LOAD httpfs",
        f"SET s3_endpoint={sql_string(parsed.netloc)}",
        f"SET s3_use_ssl={'true' if parsed.scheme == 'https' else 'false'}",
        "SET s3_url_style='path'",
        f"SET s3_region={sql_string(config.region)}",
        f"SET s3_access_key_id={sql_string(config.access_key)}",
        f"SET s3_secret_access_key={sql_string(config.secret_key)}",
        "SET s3_session_token=''",
        "SET http_proxy=''",
    )
    for statement in statements:
        try:
            connection.execute(statement)
        except Exception as error:
            raise RuntimeError(
                "DuckDB S3 test configuration failed "
                f"({type(error).__name__})"
            ) from None


def configure_s3_secret(
    connection: object,
    config: MinioConfig,
    *,
    secret_name: str = "vortex_minio",
) -> None:
    if not secret_name.replace("_", "").isalnum():
        raise AssertionError("test secret name must be an unquoted SQL identifier")
    statements = (
        "LOAD httpfs",
        f"DROP SECRET IF EXISTS {secret_name}",
        (
            f"CREATE SECRET {secret_name} ("
            "TYPE S3, "
            f"KEY_ID {sql_string(config.access_key)}, "
            f"SECRET {sql_string(config.secret_key)}, "
            f"REGION {sql_string(config.region)}, "
            f"ENDPOINT {sql_string(config.duckdb_endpoint)}, "
            f"USE_SSL {'true' if config.use_ssl else 'false'}, "
            "URL_STYLE 'path', "
            f"SCOPE {sql_string(f's3://{config.bucket}/')})"
        ),
    )
    for statement in statements:
        try:
            connection.execute(statement)
        except Exception as error:
            raise RuntimeError(
                "DuckDB S3 secret configuration failed "
                f"({type(error).__name__})"
            ) from None


def assert_credentials_absent(
    value: object, config: MinioConfig, description: str
) -> None:
    if isinstance(value, BaseException):
        chain: list[str] = []
        current: BaseException | None = value
        seen: set[int] = set()
        while current is not None and id(current) not in seen:
            seen.add(id(current))
            chain.extend((str(current), repr(current)))
            current = (
                current.__cause__
                if current.__cause__ is not None
                else current.__context__
            )
        rendered = json.dumps(chain, sort_keys=True)
    else:
        rendered = json.dumps(value, default=str, sort_keys=True)
    for credential in (config.access_key, config.secret_key):
        require_true(
            credential not in rendered,
            f"{description} contains an S3 credential",
        )


def assert_no_ambient_aws_credentials() -> None:
    forbidden = (
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_PROFILE",
        "AWS_DEFAULT_PROFILE",
        "AWS_SHARED_CREDENTIALS_FILE",
        "AWS_CONFIG_FILE",
        "AWS_WEB_IDENTITY_TOKEN_FILE",
        "AWS_ROLE_ARN",
        "AWS_ROLE_SESSION_NAME",
        "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
        "AWS_CONTAINER_CREDENTIALS_FULL_URI",
        "AWS_CONTAINER_AUTHORIZATION_TOKEN",
        "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
        "AWS_CREDENTIAL_FILE",
        "AWS_SECURITY_TOKEN",
    )
    present = sorted(name for name in forbidden if os.environ.get(name))
    require_equal(present, [], "ambient AWS credentials")
