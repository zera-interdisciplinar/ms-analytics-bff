"""Read-only clients for the operational PostgreSQL and Neo4j sources."""

from __future__ import annotations

from collections.abc import Sequence
from contextlib import closing

import pg8000.dbapi
from neo4j import Driver, GraphDatabase, ManagedTransaction, Query, READ_ACCESS, unit_of_work

from app.pipeline.types import PgColumn, PgFields, Record
from config.environments import Environments


class PostgresSource:
    def __init__(self, envs: Environments) -> None:
        if not envs.PG_HOST or not envs.PG_PASSWORD:
            raise ValueError("PG_HOST e PG_PASSWORD são obrigatórios para a extração Bronze.")
        self._kwargs: dict[str, str | int] = {
            "host": envs.PG_HOST,
            "port": int(envs.PG_PORT),
            "database": envs.PG_DATABASE,
            "user": envs.PG_USER,
            "password": envs.PG_PASSWORD,
            "timeout": 30,
        }

    def fetch_columns(self) -> list[PgColumn]:
        with closing(pg8000.dbapi.connect(**{**self._kwargs, "timeout": 15})) as connection:
            with closing(connection.cursor()) as cursor:
                cursor.execute("SET statement_timeout = 15000")
                cursor.execute("SET default_transaction_read_only = on")
                cursor.execute("SELECT 1")
                if cursor.fetchone()[0] != 1:
                    raise ConnectionError("A consulta de verificação do PostgreSQL falhou.")
                cursor.execute(
                    """
                    SELECT table_name, column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                    ORDER BY table_name, ordinal_position
                    """
                )
                return [
                    (str(tabela), str(coluna), str(tipo))
                    for tabela, coluna, tipo in cursor.fetchall()
                ]

    def fetch_entities(
        self, pg_fields: PgFields, max_rows: int,
    ) -> dict[str, list[Record]]:
        extracted: dict[str, list[Record]] = {}
        with closing(pg8000.dbapi.connect(**self._kwargs)) as connection:
            with closing(connection.cursor()) as cursor:
                cursor.execute("SET statement_timeout = 30000")
                cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                for entity, fields in pg_fields.items():
                    cols = [name for name, _ in fields]
                    projection = ", ".join('"' + coluna + '"' for coluna in cols)
                    cursor.execute(
                        "SELECT " + projection + ' FROM public."' + entity + '" '
                        "ORDER BY id LIMIT %s",
                        (max_rows + 1,),
                    )
                    values = cursor.fetchall()
                    if len(values) > max_rows:
                        raise RuntimeError(
                            f"{entity}: volume excedeu {max_rows}. "
                            "Configure extração paginada antes de continuar."
                        )
                    extracted[entity] = [dict(zip(cols, registro)) for registro in values]
        return extracted


class Neo4jSource:
    def __init__(self, envs: Environments) -> None:
        if not envs.NEO4J_URI or not envs.NEO4J_PASSWORD:
            raise ValueError(
                "NEO4J_URI e NEO4J_PASSWORD são obrigatórios para a extração Bronze."
            )
        self._uri = envs.NEO4J_URI
        self._auth = (envs.NEO4J_USER, envs.NEO4J_PASSWORD)
        self._database: str | None = envs.NEO4J_DATABASE.strip() or None

    def _driver(self) -> Driver:
        return GraphDatabase.driver(
            self._uri, auth=self._auth, connection_timeout=10,
        )

    def verify(self) -> tuple[list[str], list[str]]:
        with self._driver() as driver:
            driver.verify_connectivity()
            with driver.session(
                database=self._database, default_access_mode=READ_ACCESS,
            ) as session:
                if session.run(Query("RETURN 1 AS ok", timeout=15)).single()["ok"] != 1:
                    raise ConnectionError("A consulta de verificação do Neo4j falhou.")
                rotulos = [
                    str(linha["label"])
                    for linha in session.run(Query(
                        "CALL db.labels() YIELD label RETURN label ORDER BY label",
                        timeout=15,
                    ))
                ]
                relacionamentos = [
                    str(linha["relationshipType"])
                    for linha in session.run(Query(
                        "CALL db.relationshipTypes() YIELD relationshipType "
                        "RETURN relationshipType ORDER BY relationshipType",
                        timeout=15,
                    ))
                ]
                return rotulos, relacionamentos

    def read_graph(
        self, labels: Sequence[str], max_rows: int,
    ) -> tuple[dict[str, list[Record]], list[Record]]:
        @unit_of_work(timeout=30)
        def _read(tx: ManagedTransaction) -> tuple[dict[str, list[Record]], list[Record]]:
            nodes: dict[str, list[Record]] = {}
            for label in labels:
                consulta = (
                    "MATCH (n:`" + label + "`) "
                    "RETURN properties(n) AS p "
                    "ORDER BY coalesce(n.id, n.unitId) LIMIT $lim"
                )
                records = [registro["p"] for registro in tx.run(consulta, lim=max_rows + 1)]
                if len(records) > max_rows:
                    raise RuntimeError(f"{label}: configure paginação; limite excedido")
                nodes[label] = [
                    {
                        chave: valor
                        for chave, valor in row.items()
                        if chave not in ["embedding", "embeddedText"]
                    }
                    for row in records
                ]
            edges = [
                registro.data()
                for registro in tx.run(
                    """
                    MATCH (a)-[r]->(b)
                    WHERE type(r) IN ['IS_MODEL','BELONGS_TO','MADE_OF','HAS_EVENT','INCLUDES']
                    RETURN elementId(r) AS id, a.id AS source_id, b.id AS target_id,
                           type(r) AS relationship_type, labels(a) AS source_labels,
                           labels(b) AS target_labels, properties(r) AS properties
                    LIMIT $lim
                    """,
                    lim=max_rows + 1,
                )
            ]
            if len(edges) > max_rows:
                raise RuntimeError("Arestas: configure paginação; limite excedido")
            return nodes, edges

        with self._driver() as driver:
            with driver.session(
                database=self._database, default_access_mode=READ_ACCESS,
            ) as session:
                return session.execute_read(_read)


def live_sources(envs: Environments) -> tuple[PostgresSource, Neo4jSource]:
    return PostgresSource(envs), Neo4jSource(envs)
