from collections.abc import Sequence

import pytest

from app.pipeline.tables import NEO_LABELS, PG_FIELDS
from app.pipeline.types import PgColumn, PgFields, Record


ORG_ID = "org-1"
UNIT_ID = "unit-1"
ITEM_ID = "item-1"
MODEL_ID = "model-1"
CATEGORY_ID = "cat-1"
MATERIAL_ID = "mat-1"
EVENT_ID = "event-1"
ALERT_ID = "alert-1"
REPORT_ID = "report-1"
ADDR_ID = "addr-1"
USER_ID = "user-1"
RB_ID = "rb-1"
EXTRACTED_AT = "2026-01-15T12:00:00+00:00"
TS = "2024-06-01T10:00:00"
ACQUIRED = "2024-01-15T00:00:00"


def postgres_schema() -> list[PgColumn]:
    return [
        (table, column, tipo)
        for table, fields in PG_FIELDS.items()
        for column, tipo in fields
    ]


def postgres_tables() -> dict[str, list[Record]]:
    return {
        "organization": [{
            "id": ORG_ID, "name": "Zera", "status": "ACTIVE",
            "created_at": TS, "updated_at": TS, "plan": "FREE",
        }],
        "unit": [{
            "id": UNIT_ID, "name": "Unidade Centro", "organization_id": ORG_ID,
            "created_at": TS, "updated_at": TS,
        }],
        "user_account": [{
            "id": USER_ID, "role": "MANAGER", "status": "ACTIVE",
            "unit_id": UNIT_ID, "created_at": TS, "updated_at": TS,
            "manager_id": None,
        }],
        "address": [{
            "id": ADDR_ID, "city": "São Paulo", "state": "SP",
            "unit_id": UNIT_ID, "recycling_business_id": RB_ID,
            "created_at": TS, "updated_at": TS,
        }],
        "recycling_business": [{
            "id": RB_ID, "name": "Recicla SP", "created_at": TS, "updated_at": TS,
        }],
        "disposal_report": [{
            "id": REPORT_ID, "status": "CONFIRMED", "user_id": USER_ID,
            "unit_id": UNIT_ID, "confirmed_by": USER_ID, "confirmed_at": TS,
            "batch_id": "src-lote", "created_at": TS, "updated_at": TS,
        }],
        "alert": [{
            "id": ALERT_ID, "status": "OPEN", "unit_id": UNIT_ID,
            "created_at": TS, "updated_at": TS, "event_id": EVENT_ID,
            "rule_id": "rule-1", "user_id": USER_ID, "severity": "HIGH",
            "kind": "STOCK", "occurred_at": TS,
        }],
    }


def neo_nodes() -> dict[str, list[Record]]:
    nodes: dict[str, list[Record]] = {label: [] for label in NEO_LABELS}
    nodes["Item"] = [{
        "id": ITEM_ID, "unitId": UNIT_ID, "name": "Notebook",
        "status": "active", "condition": "good",
        "createdAt": TS, "updatedAt": TS, "acquiredAt": ACQUIRED,
        "lastEventAt": TS, "manufacturingYear": 2020, "usageIntensity": 1.0,
        "hasDamages": True, "damages": ["scratch"],
    }]
    nodes["Event"] = [{
        "id": EVENT_ID, "itemId": ITEM_ID, "unitId": UNIT_ID,
        "type": "CREATED", "toStatus": "ACTIVE", "occurredAt": TS,
    }]
    nodes["Model"] = [{
        "id": MODEL_ID, "unitId": UNIT_ID, "name": "ZBook",
        "manufacturer": "Zera", "estimatedWeightKg": 2.5,
        "expectedLifespanMonths": 36, "warrantyMonths": 12,
        "approvalStatus": "APPROVED",
    }]
    nodes["Category"] = [{"id": CATEGORY_ID, "unitId": UNIT_ID, "name": "Informática"}]
    nodes["Material"] = [{
        "id": MATERIAL_ID, "name": "Alumínio", "code": "AL",
        "recyclable": True, "hazardous": False,
    }]
    nodes["UnitInventorySettings"] = [{"unitId": UNIT_ID, "stockCapacity": 100}]
    return nodes


def neo_edges() -> list[Record]:
    return [
        {
            "id": "rel-model", "source_id": ITEM_ID, "target_id": MODEL_ID,
            "relationship_type": "IS_MODEL",
            "source_labels": ["Item"], "target_labels": ["Model"],
            "properties": {},
        },
        {
            "id": "rel-cat", "source_id": MODEL_ID, "target_id": CATEGORY_ID,
            "relationship_type": "BELONGS_TO",
            "source_labels": ["Model"], "target_labels": ["Category"],
            "properties": {},
        },
        {
            "id": "rel-mat", "source_id": MODEL_ID, "target_id": MATERIAL_ID,
            "relationship_type": "MADE_OF",
            "source_labels": ["Model"], "target_labels": ["Material"],
            "properties": {},
        },
        {
            "id": "rel-event", "source_id": ITEM_ID, "target_id": EVENT_ID,
            "relationship_type": "HAS_EVENT",
            "source_labels": ["Item"], "target_labels": ["Event"],
            "properties": {},
        },
    ]


class FakePostgres:
    def __init__(
        self,
        columns: list[PgColumn] | None = None,
        tables: dict[str, list[Record]] | None = None,
    ) -> None:
        self.columns = columns if columns is not None else postgres_schema()
        self.tables = tables if tables is not None else postgres_tables()

    def fetch_columns(self) -> list[PgColumn]:
        return list(self.columns)

    def fetch_entities(
        self, pg_fields: PgFields, max_rows: int,
    ) -> dict[str, list[Record]]:
        result: dict[str, list[Record]] = {}
        for entity, fields in pg_fields.items():
            cols = [name for name, _ in fields]
            rows = self.tables.get(entity, [])
            if len(rows) > max_rows:
                raise RuntimeError(f"{entity}: volume excedeu {max_rows}.")
            result[entity] = [{col: row[col] for col in cols} for row in rows]
        return result


class FakeNeo4j:
    def __init__(
        self,
        labels: list[str] | None = None,
        rel_types: list[str] | None = None,
        nodes: dict[str, list[Record]] | None = None,
        edges: list[Record] | None = None,
    ) -> None:
        self._labels = labels if labels is not None else list(NEO_LABELS)
        self._rel_types = rel_types if rel_types is not None else [
            "IS_MODEL", "BELONGS_TO", "MADE_OF", "HAS_EVENT",
        ]
        self._nodes = nodes if nodes is not None else neo_nodes()
        self._edges = edges if edges is not None else neo_edges()

    def verify(self) -> tuple[list[str], list[str]]:
        return list(self._labels), list(self._rel_types)

    def read_graph(
        self, labels: Sequence[str], max_rows: int,
    ) -> tuple[dict[str, list[Record]], list[Record]]:
        nodes: dict[str, list[Record]] = {}
        for label in labels:
            records = list(self._nodes.get(label, []))
            if len(records) > max_rows:
                raise RuntimeError(f"{label}: configure paginação; limite excedido")
            nodes[label] = records
        edges = list(self._edges)
        if len(edges) > max_rows:
            raise RuntimeError("Arestas: configure paginação; limite excedido")
        return nodes, edges


@pytest.fixture
def fake_postgres() -> FakePostgres:
    return FakePostgres()


@pytest.fixture
def fake_neo4j() -> FakeNeo4j:
    return FakeNeo4j()
