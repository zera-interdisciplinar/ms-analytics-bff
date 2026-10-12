"""Shared source projections used by Bronze and Silver."""

from __future__ import annotations

from app.pipeline.types import PgFields

PG_ENTITIES: tuple[str, ...] = (
    "organization",
    "unit",
    "user_account",
    "address",
    "recycling_business",
    "disposal_report",
    "alert",
)

NEO_LABELS: tuple[str, ...] = (
    "Item",
    "Event",
    "Model",
    "Category",
    "Material",
    "UnitInventorySettings",
    "Disposal",
    "Rule",
    "AlertDispatch",
)

PG_FIELDS: PgFields = {
    "organization": [
        ("id", "uuid"),
        ("name", "character varying"),
        ("status", "character varying"),
        ("created_at", "timestamp without time zone"),
        ("updated_at", "timestamp without time zone"),
        ("plan", "character varying"),
    ],
    "unit": [
        ("id", "uuid"),
        ("name", "character varying"),
        ("organization_id", "uuid"),
        ("created_at", "timestamp without time zone"),
        ("updated_at", "timestamp without time zone"),
    ],
    "user_account": [
        ("id", "uuid"),
        ("role", "character varying"),
        ("status", "character varying"),
        ("unit_id", "uuid"),
        ("created_at", "timestamp without time zone"),
        ("updated_at", "timestamp without time zone"),
        ("manager_id", "uuid"),
    ],
    "address": [
        ("id", "uuid"),
        ("city", "character varying"),
        ("state", "character varying"),
        ("unit_id", "uuid"),
        ("recycling_business_id", "uuid"),
        ("created_at", "timestamp without time zone"),
        ("updated_at", "timestamp without time zone"),
    ],
    "recycling_business": [
        ("id", "uuid"),
        ("name", "character varying"),
        ("created_at", "timestamp without time zone"),
        ("updated_at", "timestamp without time zone"),
    ],
    "disposal_report": [
        ("id", "uuid"),
        ("status", "character varying"),
        ("user_id", "uuid"),
        ("unit_id", "uuid"),
        ("confirmed_by", "uuid"),
        ("confirmed_at", "timestamp without time zone"),
        ("batch_id", "uuid"),
        ("created_at", "timestamp without time zone"),
        ("updated_at", "timestamp without time zone"),
    ],
    "alert": [
        ("id", "uuid"),
        ("status", "character varying"),
        ("unit_id", "uuid"),
        ("created_at", "timestamp without time zone"),
        ("updated_at", "timestamp without time zone"),
        ("event_id", "uuid"),
        ("rule_id", "uuid"),
        ("user_id", "uuid"),
        ("severity", "character varying"),
        ("kind", "character varying"),
        ("occurred_at", "timestamp without time zone"),
    ],
}
