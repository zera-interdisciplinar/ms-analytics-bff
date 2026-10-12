"""Orchestrates Bronze → Silver → Gold by passing pandas DataFrames in memory."""

from __future__ import annotations

from config.environments import Environments
from app.pipeline.bronze import run_bronze
from app.pipeline.gold import run_gold
from app.pipeline.silver import run_silver
from app.pipeline.sources import live_sources
from app.pipeline.types import Neo4jClient, PipelineResult, PostgresClient
from logger.logger import Logger

myLogger = Logger()


def run_pipeline(
    envs: Environments | None = None,
    postgres: PostgresClient | None = None,
    neo4j: Neo4jClient | None = None,
) -> PipelineResult:
    resolved_envs = envs or Environments()
    if postgres is None or neo4j is None:
        live_postgres, live_neo4j = live_sources(resolved_envs)
        postgres = postgres or live_postgres
        neo4j = neo4j or live_neo4j

    myLogger.Info("Running Bronze extraction")
    bronze = run_bronze(postgres, neo4j, max_rows=int(resolved_envs.PIPELINE_MAX_ROWS))
    myLogger.Info("Running Silver treatment")
    silver = run_silver(bronze)
    myLogger.Info("Running Gold KPI publication")
    gold = run_gold(silver)

    if silver["batch_id"] != bronze["batch_id"] or gold["batch_id"] != bronze["batch_id"]:
        raise ValueError("Layers diverged on batch_id")

    return {
        "batch_id": bronze["batch_id"],
        "bronze": bronze,
        "silver": silver,
        "gold": gold,
    }
