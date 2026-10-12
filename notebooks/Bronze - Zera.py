# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze - Zera | Extração das fontes
# MAGIC **Objetivo:** registrar snapshots íntegros do PostgreSQL e do Neo4j para processamento nas próximas camadas.
# MAGIC **Escopo:** entidades selecionadas e relacionamentos úteis à análise; sem limpeza, integração ou cálculo de indicadores.
# MAGIC **Campos preservados para análise:** itens e modelos, situação, danos, responsável pelo cadastro, eventos, alertas (status, gravidade e tipo), descartes (`Disposal`), composição dos materiais e a aresta `INCLUDES` (peso do item no descarte). As métricas são calculadas nas camadas Silver/Gold, não nesta extração.
# MAGIC **Fontes:** PostgreSQL `admin_core/public` e Neo4j da aplicação Zera, ambos no ambiente de testes/QA.
# MAGIC **Modelos de referência:** [SQL](https://drawsql.app/teams/zerainter/diagrams/zera) · [Neo4j](https://excalidraw.com/#json=pqbfkmMLFBerHaGGVd2sC,5r7xi5af63TOe8u72Zupqg). A seleção efetiva segue o inventário consultado na fonte.
# MAGIC **Saída:** tabelas Delta em `workspace.zera_bronze`, identificadas por lote e acompanhadas de manifesto. Os dados de QA não representam resultados de produção.

# COMMAND ----------

# MAGIC %pip install pg8000 "neo4j>=5,<6"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Acesso e preparação da extração
# MAGIC **Problema encontrado:** a extração depende de duas fontes e de credenciais configuradas no workspace; tabelas ou colunas alteradas na origem podem interromper o processo ou deixar dados de fora.
# MAGIC **Estratégia:** ler as credenciais pelo Secret Scope, testar conexões somente para leitura e conferir a estrutura disponível antes de executar as consultas de extração.
# MAGIC **Tratamento:** esta etapa não transforma dados nem cria um inventário de itens. Ela valida o acesso e o esquema das fontes.

# COMMAND ----------

# DBTITLE 1,1.1. Conexões e conferência inicial das fontes
import json
import uuid
import datetime as dt
from contextlib import closing

import pg8000.dbapi
import pandas as pd
from neo4j import GraphDatabase, READ_ACCESS, Query, unit_of_work

PG_HOST = "34.95.129.59"
PG_PORT = 15432
PG_DATABASE = "admin_core"
PG_USER = "admin_core"
NEO4J_URI = "bolt://34.95.129.59:17687"
NEO4J_USER = "neo4j"
NEO_LABELS = [
    'Item', 'Event', 'Model', 'Category', 'Material',
    'UnitInventorySettings', 'Disposal', 'Rule', 'AlertDispatch',
]

dbutils.widgets.text("secret_scope", "zera-conexoes", "Scope existente de Secrets")
dbutils.widgets.text("pg_password_key", "pg-password", "Chave da senha PostgreSQL")
dbutils.widgets.text("neo4j_password_key", "neo4j-password", "Chave da senha Neo4j")
dbutils.widgets.text("neo4j_database", "", "Banco Neo4j (vazio = padrão)")

scope = dbutils.widgets.get("secret_scope").strip()
if not scope:
    raise ValueError("Informe o scope que contém as senhas das duas fontes.")

pg_password = dbutils.secrets.get(
    scope=scope, key=dbutils.widgets.get("pg_password_key")
).rstrip("\r\n")
neo4j_password = dbutils.secrets.get(
    scope=scope, key=dbutils.widgets.get("neo4j_password_key")
).rstrip("\r\n")
neo4j_database = dbutils.widgets.get("neo4j_database").strip() or None

# O inventário é uma conferência de esquema, não uma camada de dados.
# Ele confirma os objetos disponíveis e evita consultar uma coluna que mudou de nome.
with closing(pg8000.dbapi.connect(
    host=PG_HOST, port=PG_PORT, database=PG_DATABASE, user=PG_USER,
    password=pg_password, timeout=15,
)) as connection:
    with closing(connection.cursor()) as cursor:
        cursor.execute("SET statement_timeout = 15000")
        cursor.execute("SET default_transaction_read_only = on")
        cursor.execute("SELECT 1")
        if cursor.fetchone()[0] != 1:
            raise ConnectionError("A consulta de verificação do PostgreSQL falhou.")
        cursor.execute("""
            SELECT table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public'
            ORDER BY table_name, ordinal_position
        """)
        colunas_postgres = cursor.fetchall()

with GraphDatabase.driver(
    NEO4J_URI, auth=(NEO4J_USER, neo4j_password), connection_timeout=10,
) as driver:
    driver.verify_connectivity()
    with driver.session(
        database=neo4j_database, default_access_mode=READ_ACCESS,
    ) as session:
        if session.run(Query("RETURN 1 AS ok", timeout=15)).single()["ok"] != 1:
            raise ConnectionError("A consulta de verificação do Neo4j falhou.")
        rotulos_neo4j = [
            linha["label"] for linha in session.run(Query(
                "CALL db.labels() YIELD label RETURN label ORDER BY label",
                timeout=15,
            ))
        ]
        relacionamentos_neo4j = [
            linha["relationshipType"] for linha in session.run(Query(
                "CALL db.relationshipTypes() YIELD relationshipType "
                "RETURN relationshipType ORDER BY relationshipType",
                timeout=15,
            ))
        ]

print("PostgreSQL: conexão e consulta somente leitura OK.")
print("Neo4j: conexão e consulta somente leitura OK.")
print("Rótulos Neo4j encontrados:", rotulos_neo4j)
print("Relacionamentos Neo4j encontrados:", relacionamentos_neo4j)
rotulos_ausentes = sorted(set(NEO_LABELS) - set(rotulos_neo4j))
if rotulos_ausentes:
    raise ValueError("Rótulos esperados ausentes no Neo4j: " + ", ".join(rotulos_ausentes))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2. Funções de leitura e gravação Delta
# MAGIC **Estratégia:** centralizar a conversão entre pandas e Delta para manter o mesmo tratamento de tipos, nulos e tabelas vazias em todas as entidades do lote.

# COMMAND ----------

# DBTITLE 1,1.2. Funções de armazenamento Delta
import numpy as np
from datetime import date, datetime
from pyspark.sql import types as T

limite_linhas = 100_000

def ler_delta(tabela, lote=None):
    consulta = f"SELECT * FROM {tabela}"
    if lote is not None:
        consulta += f" WHERE batch_id = '{lote}'"
    df = spark.sql(consulta).limit(limite_linhas + 1).toPandas()
    if len(df) > limite_linhas:
        raise ValueError(f"{tabela}: volume excede o limite desta análise pandas.")
    return df

def salvar_delta(df, tabela, modo='overwrite'):
    # O esquema explícito permite salvar tabelas vazias e colunas totalmente nulas.
    campos = []
    for coluna in df.columns:
        serie = df[coluna]
        valores = serie.dropna()
        exemplo = valores.iloc[0] if len(valores) else None
        if pd.api.types.is_datetime64_any_dtype(serie):
            tipo = T.TimestampNTZType()
        elif pd.api.types.is_bool_dtype(serie) or isinstance(exemplo, (bool, np.bool_)):
            tipo = T.BooleanType()
        elif pd.api.types.is_integer_dtype(serie):
            tipo = T.LongType()
        elif pd.api.types.is_numeric_dtype(serie):
            tipo = T.DoubleType()
        elif isinstance(exemplo, (list, np.ndarray)):
            tipo = T.ArrayType(T.StringType())
        elif isinstance(exemplo, date) and not isinstance(exemplo, datetime):
            tipo = T.DateType()
        else:
            tipo = T.StringType()
        campos.append(T.StructField(coluna, tipo, True))

    def valor_delta(valor, tipo):
        if isinstance(tipo, T.ArrayType):
            return list(valor) if isinstance(valor, (list, np.ndarray)) else None
        if pd.isna(valor):
            return None
        if isinstance(tipo, T.TimestampNTZType):
            return pd.Timestamp(valor).to_pydatetime()
        if isinstance(tipo, T.BooleanType):
            return bool(valor)
        if isinstance(tipo, T.LongType):
            return int(valor)
        if isinstance(tipo, T.DoubleType):
            return float(valor)
        return str(valor) if isinstance(tipo, T.StringType) else valor

    linhas = [tuple(valor_delta(v, c.dataType) for v, c in zip(linha, campos))
              for linha in df.itertuples(index=False, name=None)]
    dados_delta = spark.createDataFrame(linhas, T.StructType(campos))
    if spark.catalog.tableExists(tabela):
        spark.sql(f"ALTER TABLE {tabela} SET TBLPROPERTIES ('delta.feature.timestampNtz'='supported')")
    dados_delta.write.format('delta').mode(modo).option('overwriteSchema', 'true').saveAsTable(tabela)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Preparação do lote e regras de extração
# MAGIC **Problema encontrado:** extrações de tabelas diferentes precisam ser identificadas como parte do mesmo snapshot para que a Silver não misture cargas incompletas.
# MAGIC **Estratégia:** gerar um identificador de lote, registrar o horário UTC, limitar o volume por entidade e gravar cada payload sem alterar os valores de origem.
# MAGIC **Privacidade:** selecionar apenas os campos necessários à análise e deixar de fora credenciais, tokens, convites e contatos pessoais.

# COMMAND ----------

# DBTITLE 1,2. Preparação do lote e acesso seguro às fontes
spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.zera_bronze")
batch_id = str(uuid.uuid4())
extracted_at = dt.datetime.now(dt.timezone.utc).isoformat()
max_rows = 100000
raw_schema = "entity_key STRING, payload_json STRING, batch_id STRING, extracted_at STRING"
inventory = []
dados_brutos = {}

def save_raw(entity, records):
    # A extração vira um DataFrame pandas; o JSON preserva propriedades da fonte.
    df = pd.DataFrame(records)
    dados_brutos[entity] = df
    linhas = pd.DataFrame({
        'entity_key': [str(r.get('id') or r.get('unitId') or '') for r in records],
        'payload_json': [json.dumps(r, ensure_ascii=False, default=str) for r in records],
        'batch_id': [batch_id] * len(records),
        'extracted_at': [extracted_at] * len(records),
    }).astype('string')
    salvar_delta(linhas, 'workspace.zera_bronze.' + entity, modo='append')
    inventory.append((batch_id, extracted_at, entity, len(df)))
    print(f"{entity}: {len(df)} registros extraídos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Extração PostgreSQL
# MAGIC **Problema encontrado:** trazer todas as colunas da aplicação pode expor dados pessoais e informações que não são necessárias para a análise.
# MAGIC **Estratégia:** projetar somente as colunas selecionadas do schema `public`, validar a projeção contra o esquema atual e ler as tabelas em transação somente leitura.
# MAGIC **Validação:** a consulta falha se uma coluna esperada não existir ou se o volume ultrapassar o limite definido para esta versão.

# COMMAND ----------

# DBTITLE 1,3. Extração PostgreSQL | Cadastros e registros operacionais
# Não ingere senhas, tokens, convites, dados de migração nem contatos pessoais.
PG_FIELDS = {
    'organization': [
        ('id', 'uuid'), ('name', 'character varying'),
        ('status', 'character varying'), ('created_at', 'timestamp without time zone'),
        ('updated_at', 'timestamp without time zone'), ('plan', 'character varying'),
    ],
    'unit': [
        ('id', 'uuid'), ('name', 'character varying'), ('organization_id', 'uuid'),
        ('created_at', 'timestamp without time zone'),
        ('updated_at', 'timestamp without time zone'),
    ],
    'user_account': [
        ('id', 'uuid'), ('role', 'character varying'), ('status', 'character varying'),
        ('unit_id', 'uuid'), ('created_at', 'timestamp without time zone'),
        ('updated_at', 'timestamp without time zone'), ('manager_id', 'uuid'),
    ],
    'address': [
        ('id', 'uuid'), ('city', 'character varying'), ('state', 'character varying'),
        ('unit_id', 'uuid'), ('recycling_business_id', 'uuid'),
        ('created_at', 'timestamp without time zone'),
        ('updated_at', 'timestamp without time zone'),
    ],
    'recycling_business': [
        ('id', 'uuid'), ('name', 'character varying'),
        ('created_at', 'timestamp without time zone'),
        ('updated_at', 'timestamp without time zone'),
    ],
    'disposal_report': [
        ('id', 'uuid'), ('status', 'character varying'), ('user_id', 'uuid'),
        ('unit_id', 'uuid'), ('confirmed_by', 'uuid'),
        ('confirmed_at', 'timestamp without time zone'), ('batch_id', 'uuid'),
        ('created_at', 'timestamp without time zone'),
        ('updated_at', 'timestamp without time zone'),
    ],
    'alert': [
        ('id', 'uuid'), ('status', 'character varying'), ('unit_id', 'uuid'),
        ('created_at', 'timestamp without time zone'),
        ('updated_at', 'timestamp without time zone'), ('event_id', 'uuid'),
        ('rule_id', 'uuid'), ('user_id', 'uuid'), ('severity', 'character varying'),
        ('kind', 'character varying'), ('occurred_at', 'timestamp without time zone'),
    ],
}
colunas_disponiveis = {(tabela, coluna) for tabela, coluna, _ in colunas_postgres}
colunas_ausentes = [
    f"public.{tabela}.{coluna}"
    for tabela, campos in PG_FIELDS.items()
    for coluna, _ in campos
    if (tabela, coluna) not in colunas_disponiveis
]
if colunas_ausentes:
    raise ValueError("O esquema PostgreSQL mudou; revisar a projeção: " + ", ".join(colunas_ausentes))

esquema_selecionado = pd.DataFrame(
    [
        [tabela, coluna, tipo]
        for tabela, campos in PG_FIELDS.items()
        for coluna, _ in campos
        for tabela_disponivel, coluna_disponivel, tipo in colunas_postgres
        if (tabela, coluna) == (tabela_disponivel, coluna_disponivel)
    ],
    columns=["tabela", "coluna", "tipo_origem"],
)
display(esquema_selecionado)

with closing(pg8000.dbapi.connect(
    host=PG_HOST, port=PG_PORT, database=PG_DATABASE, user=PG_USER,
    password=pg_password, timeout=30,
)) as connection:
    with closing(connection.cursor()) as cursor:
        cursor.execute("SET statement_timeout = 30000")
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        for entity, fields in PG_FIELDS.items():
            cols = [name for name, _ in fields]
            projection = ', '.join('"' + coluna + '"' for coluna in cols)
            cursor.execute(
                'SELECT ' + projection + ' FROM public."' + entity + '" '
                'ORDER BY id LIMIT %s',
                (max_rows + 1,),
            )
            values = cursor.fetchall()
            if len(values) > max_rows:
                raise RuntimeError(
                    f"{entity}: volume excedeu {max_rows}. "
                    "Configure extração paginada antes de continuar."
                )
            save_raw(
                'pg_' + entity,
                [dict(zip(cols, registro)) for registro in values],
            )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Extração Neo4j
# MAGIC **Problema encontrado:** o modelo de grafos representa entidades e relações de forma diferente das tabelas relacionais; achatar essas relações nesta etapa esconderia a estrutura da fonte.
# MAGIC **Estratégia:** preservar nós por rótulo e relacionamentos em uma entidade separada (`IS_MODEL`, `BELONGS_TO`, `MADE_OF`, `HAS_EVENT`, `INCLUDES`), removendo apenas propriedades de embedding que não são usadas no BI.
# MAGIC **Validação:** conferir os rótulos esperados e interromper a extração se uma entidade ou o conjunto de relações exceder o limite configurado.

# COMMAND ----------

# DBTITLE 1,4. Extração Neo4j | Entidades e relacionamentos
# As propriedades operacionais são preservadas; vetores e texto de embeddings não são usados no BI.
@unit_of_work(timeout=30)
def read_graph(tx):
    nodes = {}
    for label in NEO_LABELS:
        consulta = (
            'MATCH (n:`' + label + '`) '
            'RETURN properties(n) AS p '
            'ORDER BY coalesce(n.id, n.unitId) LIMIT $lim'
        )
        records = [
            registro['p']
            for registro in tx.run(consulta, lim=max_rows + 1)
        ]
        if len(records) > max_rows:
            raise RuntimeError(f"{label}: configure paginação; limite excedido")
        nodes[label] = [
            {
                chave: valor
                for chave, valor in row.items()
                if chave not in ['embedding', 'embeddedText']
            }
            for row in records
        ]
    edges = [registro.data() for registro in tx.run("""
      MATCH (a)-[r]->(b)
      WHERE type(r) IN ['IS_MODEL','BELONGS_TO','MADE_OF','HAS_EVENT','INCLUDES']
      RETURN elementId(r) AS id, a.id AS source_id, b.id AS target_id,
             type(r) AS relationship_type, labels(a) AS source_labels,
             labels(b) AS target_labels, properties(r) AS properties
      LIMIT $lim
    """, lim=max_rows + 1)]
    if len(edges) > max_rows:
        raise RuntimeError('Arestas: configure paginação; limite excedido')
    return nodes, edges

with GraphDatabase.driver(NEO4J_URI,auth=(NEO4J_USER,neo4j_password),connection_timeout=10) as driver:
    with driver.session(
        database=neo4j_database,
        default_access_mode=READ_ACCESS,
    ) as session:
        nodes, edges = session.execute_read(read_graph)
for label, records in nodes.items():
    save_raw('neo_' + label.lower(), records)
save_raw('neo_relationships', edges)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Publicação e manifesto do lote
# MAGIC **Problema encontrado:** se a execução parar no meio, tabelas parciais não podem ser confundidas com uma extração completa.
# MAGIC **Estratégia:** publicar o manifesto somente depois das duas fontes, com contagem de linhas por entidade e o mesmo identificador de lote usado nos snapshots.

# COMMAND ----------

# DBTITLE 1,5. Publicação do lote e manifesto de controle
# Silver só consome lotes com manifesto completo; extrações interrompidas não são publicadas.
manifest = pd.DataFrame(
    inventory,
    columns=['batch_id', 'extracted_at', 'entity', 'row_count'],
)
salvar_delta(manifest, 'workspace.zera_bronze.manifest', modo='append')
display(manifest[['entity', 'row_count']])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Apresentação e diagnóstico da extração
# MAGIC **Problema encontrado:** um manifesto concluído não mostra, por si só, se alguma entidade veio vazia ou se houve diferença de cobertura entre as fontes. 
# MAGIC **Estratégia:** verificar a cobertura e a conclusão do lote recém-extraído. 
# MAGIC **O que cada linha representa:** um registro de uma entidade na captura; os relacionamentos ficam em tabelas próprias.
# MAGIC **Verificações:** contagens por entidade, entidades vazias e identificação do lote.
# MAGIC **Limites:** cada origem é lida em transação própria; se o limite de linhas for excedido, a execução falha sem publicar um manifesto completo.
# MAGIC **Leitura gerencial:** ausência de alertas ou descartes registrados não comprova eficiência operacional. Os dados atuais são de QA.

# COMMAND ----------

# DBTITLE 1,Resumo de cobertura e verificação do lote
display(manifest[['entity', 'row_count', 'extracted_at']].sort_values('entity'))
print('Lote:', batch_id)
print('Entidades vazias:', [entity for _, _, entity, n in inventory if n == 0])
print('Senhas/tokens/contatos pessoais e vetores de embedding não são insumos desta análise.')


contagens = {
    entity: count
    for _, _, entity, count in inventory
}
dbutils.notebook.exit(json.dumps(
    {'batch_id': batch_id, 'counts': contagens},
    ensure_ascii=False,
))
