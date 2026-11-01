# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Silver - Zera | Tratamento e integração
# MAGIC **Objetivo:** validar os snapshots Bronze, padronizar tipos e integrar itens, modelos, categorias, unidades, organizações e materiais.
# MAGIC **Problema analisado:** campos incompletos, valores inconsistentes e chaves sem correspondência podem distorcer os indicadores.
# MAGIC **Estratégia:** perfilar as fontes, preservar nulos, normalizar textos e datas, validar cardinalidades dos relacionamentos e registrar alertas sem excluir registros por suposição.
# MAGIC **O que cada linha representa:** um item cadastrado, inclusive itens `DRAFT`; isso não equivale a estoque ativo. Eventos e composição de materiais permanecem em tabelas próprias.
# MAGIC **Campos para os indicadores:** mantém a situação dos alertas, a indicação de danos, o responsável pelo cadastro, os descartes (`destination`, `disposedAt`, `cancelled`) e a classificação de reciclabilidade dos materiais; calcula o potencial reciclável por item sem confundi-lo com reciclagem efetiva.
# MAGIC **Implementação:** transformações com pandas; Spark é usado somente para ler e gravar Delta. Limite de 100.000 linhas por tabela nesta versão.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Preparação e leitura Delta
# MAGIC **Problema encontrado:** os snapshots Bronze são armazenados em Delta e precisam ser lidos com controle de volume para permitir o tratamento em pandas.
# MAGIC **Estratégia:** concentrar as funções comuns de leitura e gravação, limitar cada base a 100.000 linhas e manter o Spark somente para operações Delta.

# COMMAND ----------

# DBTITLE 1,1. Dependências, leitura e gravação Delta
import json
from datetime import date, datetime

import numpy as np
import pandas as pd
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

    linhas = [
        tuple(valor_delta(valor, campo.dataType) for valor, campo in zip(linha, campos))
        for linha in df.itertuples(index=False, name=None)
    ]
    dados_delta = spark.createDataFrame(linhas, T.StructType(campos))
    if spark.catalog.tableExists(tabela):
        spark.sql(f"ALTER TABLE {tabela} SET TBLPROPERTIES ('delta.feature.timestampNtz'='supported')")
    dados_delta.write.format('delta').mode(modo).option('overwriteSchema', 'true').saveAsTable(tabela)

spark.sql('CREATE SCHEMA IF NOT EXISTS workspace.zera_silver')

def contar_linhas_duplicadas(df):
    """Conta linhas repetidas sem falhar com listas ou dicionários nas células."""
    dados_comparacao = df.copy()
    for coluna in dados_comparacao.columns:
        serie = dados_comparacao[coluna]
        if pd.api.types.is_object_dtype(serie.dtype):
            contem_valores_aninhados = serie.map(
                lambda valor: isinstance(valor, (list, dict, set, np.ndarray))
            ).any()
            if contem_valores_aninhados:
                dados_comparacao[coluna] = serie.map(
                    lambda valor: json.dumps(
                        valor, ensure_ascii=False, sort_keys=True, default=str
                    ) if isinstance(valor, (list, dict, set, np.ndarray)) else valor
                )
    return int(dados_comparacao.duplicated().sum())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Seleção do snapshot Bronze
# MAGIC **Problema encontrado:** os dados de cada execução são gravados em lotes separados; misturar lotes pode combinar cadastros e eventos de momentos diferentes.
# MAGIC **Estratégia:** escolher o lote mais recente pelo horário de extração, confirmar que há manifesto e usar o mesmo `batch_id` em todas as leituras.

# COMMAND ----------

# DBTITLE 1,2.1. Identificação do lote Bronze mais recente
manifestos = ler_delta('workspace.zera_bronze.manifest')
if manifestos.empty:
    raise ValueError('Execute a Bronze primeiro.')
ultimo_lote = manifestos.sort_values(['extracted_at', 'batch_id']).iloc[-1]
batch_id = ultimo_lote['batch_id']
manifest = manifestos.loc[manifestos['batch_id'].eq(batch_id)].copy()
data_referencia = pd.Timestamp(ultimo_lote['extracted_at']).tz_convert('UTC').tz_localize(None).normalize()
bases = {}
verificacoes = []
print('Lote:', batch_id, '| Referência:', data_referencia.date())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Leitura e diagnóstico das fontes
# MAGIC **Problema:** campos opcionais podem estar ausentes, entidades podem vir vazias e registros duplicados podem comprometer as integrações.
# MAGIC **Estratégia:** conferir as contagens contra o manifesto Bronze, apresentar uma amostra, medir tipos, nulos e duplicatas e validar as chaves antes de qualquer padronização.
# MAGIC **Regra de qualidade:** chave ausente ou repetida interrompe o pipeline; não escolhemos um registro arbitrariamente. Valores ausentes em atributos opcionais são preservados e documentados no perfil.

# COMMAND ----------

# DBTITLE 1,3.1. Leitura do snapshot e validação das chaves
# A função mantém a origem identificável, remove espaços excedentes de texto,
# confere as contagens contra o manifesto e interrompe o fluxo para chaves inválidas.
def carregar_entidade(entidade, colunas):
    bruto = ler_delta('workspace.zera_bronze.' + entidade, batch_id)
    quantidade_esperada = int(manifest.loc[manifest['entity'].eq(entidade), 'row_count'].iloc[0])
    if len(bruto) != quantidade_esperada:
        raise ValueError('Contagem diferente do manifesto: ' + entidade)
    registros = [json.loads(texto) for texto in bruto['payload_json']]
    df = pd.DataFrame(registros).reindex(columns=colunas)
    df = df.rename(columns={'batch_id': 'lote_origem_id'})
    df['batch_id'] = batch_id
    df['extracted_at'] = ultimo_lote['extracted_at']
    chave = 'id' if 'id' in df.columns else 'unitId'
    ausentes = int((df[chave].isna() | df[chave].astype('string').str.strip().eq('')).sum())
    repetidos = int(df[chave].duplicated().sum())
    verificacoes.extend([(entidade, 'chave_ausente', ausentes), (entidade, 'chave_duplicada', repetidos)])
    if ausentes or repetidos:
        raise ValueError('Chave inválida em ' + entidade)
    return df

def converter_data(df, coluna, entidade):
    origem = df[coluna].copy()
    nativas = origem.map(
        lambda valor: valor.to_native() if hasattr(valor, 'to_native') else valor
    )
    datas = pd.to_datetime(nativas, errors='coerce', format='mixed')
    invalidas = int((origem.notna() & datas.isna()).sum())
    nomes_datas = {
        'created_at': 'data_criacao_sql',
        'updated_at': 'data_atualizacao_sql',
        'confirmed_at': 'data_confirmacao_sql',
        'occurred_at': 'data_ocorrencia_sql',
        'createdAt': 'data_cadastro',
        'updatedAt': 'data_atualizacao',
        'acquiredAt': 'data_aquisicao',
        'lastEventAt': 'data_ultimo_evento',
        'occurredAt': 'data_ocorrencia',
    }
    nome_verificacao = 'datas_invalidas_' + nomes_datas.get(coluna, coluna)
    verificacoes.append((entidade, nome_verificacao, invalidas))
    print(
        f'{entidade}.{coluna} | tipo convertido: {datas.dtype} '
        f'| datas inválidas: {invalidas}'
    )
    if invalidas:
        raise ValueError('Data inválida: ' + entidade + '.' + coluna)
    if isinstance(datas.dtype, pd.DatetimeTZDtype):
        raise ValueError('Confirmar fuso da fonte antes de converter: ' + coluna)
    df[coluna] = datas
    datas_validas = datas.dropna()
    if datas_validas.empty:
        print('Intervalo temporal: sem datas válidas.')
    else:
        print(
            'Intervalo temporal:',
            datas_validas.min(), 'até', datas_validas.max(),
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2. Cadastros PostgreSQL: leitura do snapshot
# MAGIC **Problema encontrado:** tabelas relacionais podem ter campos opcionais, chaves sem correspondência e dados ausentes.
# MAGIC **Estratégia:** selecionar as colunas necessárias, validar chaves e contagens e manter os valores brutos para que o diagnóstico inicial descreva a origem.

# COMMAND ----------

# DBTITLE 1,3.2. Leitura dos cadastros PostgreSQL
colunas_postgres = {
    'address': [
        'id', 'city', 'state', 'unit_id', 'recycling_business_id',
        'created_at', 'updated_at',
    ],
    'alert': [
        'id', 'status', 'unit_id', 'created_at', 'updated_at', 'event_id',
        'rule_id', 'user_id', 'severity', 'kind', 'occurred_at',
    ],
    'disposal_report': [
        'id', 'status', 'user_id', 'unit_id', 'confirmed_by', 'confirmed_at',
        'batch_id', 'created_at', 'updated_at',
    ],
    'organization': ['id', 'name', 'status', 'created_at', 'updated_at', 'plan'],
    'recycling_business': ['id', 'name', 'created_at', 'updated_at'],
    'unit': ['id', 'name', 'organization_id', 'created_at', 'updated_at'],
    'user_account': [
        'id', 'role', 'status', 'unit_id', 'created_at', 'updated_at', 'manager_id',
    ],
}
for entidade, colunas in colunas_postgres.items():
    df = carregar_entidade('pg_' + entidade, colunas)
    bases['pg_' + entidade] = df

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3. Entidades Neo4j: leitura do snapshot
# MAGIC **Problema encontrado:** nós e relações representam partes diferentes do grafo; achatar ou tratar os dados antes do diagnóstico pode esconder chaves ausentes e duplicatas.
# MAGIC **Estratégia:** carregar cada entidade e os relacionamentos em bases separadas, preservando os valores originais para a visão geral.

# COMMAND ----------

# DBTITLE 1,3.3. Leitura do inventário Neo4j
colunas_neo4j = {
    'category': ['id', 'unitId', 'name'],
    'event': ['id', 'itemId', 'unitId', 'type', 'toStatus', 'occurredAt'],
    'item': [
        'id', 'unitId', 'name', 'status', 'condition', 'createdAt', 'updatedAt',
        'acquiredAt', 'lastEventAt', 'manufacturingYear', 'usageIntensity',
        'hasDamages', 'damages', 'createdBy', 'createdByName',
    ],
    'disposal': [
        'id', 'unitId', 'destination', 'disposedAt', 'cancelled', 'createdAt',
    ],
    'material': ['id', 'name', 'code', 'recyclable', 'hazardous'],
    'model': [
        'id', 'unitId', 'name', 'manufacturer', 'estimatedWeightKg',
        'expectedLifespanMonths', 'warrantyMonths', 'approvalStatus',
    ],
    'relationships': ['id', 'source_id', 'target_id', 'relationship_type'],
    'unitinventorysettings': ['unitId', 'stockCapacity'],
}
for entidade, colunas in colunas_neo4j.items():
    df = carregar_entidade('neo_' + entidade, colunas)
    bases['neo_' + entidade] = df

# COMMAND ----------

# DBTITLE 1,3.4. Perfil de tipos, nulos e distribuição inicial
perfis = []
for entidade, df in bases.items():
    duplicatas_linha_completa = contar_linhas_duplicadas(df)
    for coluna in df.columns:
        if coluna not in ['batch_id', 'extracted_at']:
            nulos = int(df[coluna].isna().sum())
            perfis.append([
                entidade, coluna, str(df[coluna].dtype), len(df), nulos,
                nulos / len(df) * 100 if len(df) else np.nan,
                duplicatas_linha_completa,
            ])
perfil = pd.DataFrame(perfis, columns=[
    'entidade', 'coluna_origem', 'tipo', 'linhas', 'nulos',
    'percentual_nulos', 'duplicatas_linha_completa',
])
salvar_delta(perfil, 'workspace.zera_silver.perfil_preenchimento')
print('Visão geral das bases carregadas:')
resumo_bases = (
    perfil.groupby('entidade', dropna=False)
    .agg(linhas=('linhas', 'max'), colunas=('coluna_origem', 'nunique'),
         duplicatas_linha_completa=('duplicatas_linha_completa', 'max'))
    .reset_index()
)
display(resumo_bases)
print('Amostra dos itens antes da integração:')
display(bases['neo_item'].head(5))
display(perfil.sort_values('percentual_nulos', ascending=False).head(80))
display(bases['neo_item'][['manufacturingYear', 'usageIntensity']].describe())
display(bases['neo_item'][['status', 'condition']].value_counts(dropna=False).rename('quantidade').reset_index())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Conversão e padronização dos tipos
# MAGIC **Problema encontrado:** datas, medidas numéricas e atributos booleanos podem chegar com tipos diferentes entre fontes; converter sem diagnóstico pode transformar valores inválidos em nulos sem aviso.
# MAGIC **Estratégia:** remover espaços excedentes de textos, converter datas e números com validação do valor original, preservar valores ausentes e registrar cada contagem de invalidez.
# MAGIC **Fuso horário:** datas sem fuso permanecem sem fuso; se a origem trouxer fuso, o fluxo interrompe a conversão até que a regra seja confirmada.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1. Textos e datas do PostgreSQL
# MAGIC **Problema encontrado:** espaços excedentes e datas inválidas podem criar chaves sem correspondência ou tornar as análises temporais incorretas.
# MAGIC **Estratégia:** remover espaços nas pontas de valores textuais, converter todas as colunas temporais selecionadas e interromper o tratamento quando a origem contém uma data inválida.

# COMMAND ----------

# DBTITLE 1,4.1. Padronização de textos e conversão de datas PostgreSQL
for df in bases.values():
    for coluna in df.columns:
        if coluna != 'damages':
            df[coluna] = df[coluna].map(
                lambda valor: valor.strip() if isinstance(valor, str) else valor
            )

for entidade, colunas in colunas_postgres.items():
    df = bases['pg_' + entidade]
    if entidade == 'alert':
        # Os valores considerados resolvidos são definidos na Gold após confirmar o enum.
        df['status'] = df['status'].astype('string').str.upper()
    for coluna in colunas:
        if coluna.endswith('_at'):
            converter_data(df, coluna, 'pg_' + entidade)
    salvar_delta(df, 'workspace.zera_silver.pg_' + entidade)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2. Datas e tipos Neo4j
# MAGIC **Problema encontrado:** medidas e classificações ambientais podem ser recebidas como texto ou com valores incompatíveis com o tipo esperado.
# MAGIC **Estratégia:** validar números e booleanos antes de publicar as entidades tratadas; manter nulos e registrar a cobertura das classificações de reciclabilidade.

# COMMAND ----------

# DBTITLE 1,4.2. Conversão de datas, números e booleanos Neo4j
colunas_numericas = [
    'manufacturingYear', 'usageIntensity', 'estimatedWeightKg',
    'expectedLifespanMonths', 'warrantyMonths', 'stockCapacity',
]
colunas_booleanas = ['hasDamages', 'recyclable', 'hazardous', 'cancelled']
colunas_datas_neo4j = {
    'item': ['createdAt', 'updatedAt', 'acquiredAt', 'lastEventAt'],
    'event': ['occurredAt'],
    'disposal': ['disposedAt', 'createdAt'],
}

for entidade, colunas in colunas_neo4j.items():
    df = bases['neo_' + entidade]
    for coluna in colunas_datas_neo4j.get(entidade, []):
        converter_data(df, coluna, 'neo_' + entidade)
    for coluna in set(colunas) & set(colunas_numericas):
        origem = df[coluna].copy()
        df[coluna] = pd.to_numeric(origem, errors='coerce').astype('Float64')
        invalidos = int((origem.notna() & df[coluna].isna()).sum())
        print(
            f'neo_{entidade}.{coluna} | tipo convertido: {df[coluna].dtype} '
            f'| valores inválidos: {invalidos}'
        )
        if invalidos:
            raise ValueError('Número inválido: ' + entidade + '.' + coluna)
    for coluna in set(colunas) & set(colunas_booleanas):
        validos = df[coluna].dropna().map(
            lambda valor: isinstance(valor, bool)
        )
        invalidos = int((~validos).sum())
        print(
            f'neo_{entidade}.{coluna} | tipo esperado: boolean '
            f'| valores inválidos: {invalidos}'
        )
        if invalidos:
            raise ValueError('Booleano inválido: ' + entidade + '.' + coluna)
        df[coluna] = df[coluna].astype('boolean')
    if entidade == 'material':
        verificacoes.append((
            entidade,
            'classificacao_reciclabilidade_ausente',
            int(df['recyclable'].isna().sum()),
        ))
    salvar_delta(df, 'workspace.zera_silver.neo_' + entidade)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Integração do inventário e da composição
# MAGIC **Problema:** unir materiais diretamente aos itens pode multiplicar linhas e inflar contagens.
# MAGIC **Estratégia:** relacionar cada item ao modelo, categoria e unidade com validação de cardinalidade; escolher o endereço mais recente por unidade, como documentado no Confluence; resumir materiais por modelo antes de integrar os indicadores de composição.
# MAGIC **Padronização:** uniformizar status e condição. A renomeação analítica fica para a etapa seguinte, depois que as junções usam as chaves técnicas originais.
# MAGIC **Exceções:** itens sem modelo ou unidade correspondente permanecem na base e recebem sinalizadores para revisão.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1. Integração dos cadastros
# MAGIC **Estratégia:** manter uma linha por item, validar cada junção com a cardinalidade esperada e escolher um endereço por unidade para evitar multiplicação de linhas.

# COMMAND ----------

# DBTITLE 1,5.1. Integração de itens e cadastros
itens = bases['neo_item'].copy()
itens['status'] = itens['status'].astype('string').str.upper()
itens['condition'] = itens['condition'].astype('string').str.upper()
relacoes = bases['neo_relationships']
item_modelo = (
    relacoes.loc[
        relacoes['relationship_type'].eq('IS_MODEL'), ['source_id', 'target_id']
    ]
    .rename(columns={'source_id': 'id', 'target_id': 'model_id'})
)
modelo_categoria = (
    relacoes.loc[
        relacoes['relationship_type'].eq('BELONGS_TO'), ['source_id', 'target_id']
    ]
    .rename(columns={'source_id': 'model_id', 'target_id': 'category_id'})
)
modelos = (
    bases['neo_model'][[
        'id', 'name', 'manufacturer', 'estimatedWeightKg',
        'expectedLifespanMonths', 'warrantyMonths',
    ]]
    .rename(columns={'id': 'model_id', 'name': 'model_name'})
)
categorias = (
    bases['neo_category'][['id', 'name']]
    .rename(columns={'id': 'category_id', 'name': 'category_name'})
)
unidades = (
    bases['pg_unit'][['id', 'name', 'organization_id']]
    .rename(columns={'id': 'unitId', 'name': 'unit_name'})
)
organizacoes = (
    bases['pg_organization'][['id', 'name']]
    .rename(columns={'id': 'organization_id', 'name': 'organization_name'})
)
enderecos = bases['pg_address'][
    ['id', 'unit_id', 'city', 'state', 'created_at']
].dropna(subset=['unit_id']).copy()
enderecos_com_historico = enderecos['unit_id'].duplicated(keep=False)
unidades_com_multiplos_enderecos = int(
    enderecos.loc[enderecos_com_historico, 'unit_id'].nunique()
)
verificacoes.append((
    'pg_address', 'unidades_com_multiplos_enderecos',
    unidades_com_multiplos_enderecos,
))
# A documentação recomenda o endereço mais recente para evitar fan-out.
enderecos = (
    enderecos.sort_values(
        ['unit_id', 'created_at', 'id'], na_position='first'
    )
    .drop_duplicates('unit_id', keep='last')
    .rename(columns={'unit_id': 'unitId'})
)
itens = itens.merge(item_modelo, on='id', how='left', validate='one_to_one')
itens = itens.merge(modelos, on='model_id', how='left', validate='many_to_one')
itens = itens.merge(modelo_categoria, on='model_id', how='left', validate='many_to_one')
itens = itens.merge(categorias, on='category_id', how='left', validate='many_to_one')
itens = itens.merge(unidades, on='unitId', how='left', validate='many_to_one')
itens = itens.merge(organizacoes, on='organization_id', how='left', validate='many_to_one')
itens = itens.merge(
    enderecos[['unitId', 'city', 'state']], on='unitId', how='left',
    validate='many_to_one',
)
itens['unidade_encontrada'] = itens['unit_name'].notna()
itens['modelo_encontrado'] = itens['model_name'].notna()
eventos = bases['neo_event'].copy()
verificacoes.append(('eventos', 'item_nao_encontrado', int((~eventos['itemId'].isin(itens['id'])).sum())))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2. Composição de materiais
# MAGIC **Problema encontrado:** um modelo pode ter vários materiais; unir essa relação diretamente aos itens repetiria o mesmo item e inflaria contagens.
# MAGIC **Estratégia:** relacionar materiais a modelos, resumir a classificação por modelo e só então incorporar esses atributos aos itens.

# COMMAND ----------

# DBTITLE 1,5.2. Composição de materiais sem multiplicar itens
materiais = bases['neo_material'][['id','name','recyclable','hazardous']].rename(columns={'id':'material_id'})
composicao = (
    relacoes.loc[
        relacoes['relationship_type'].eq('MADE_OF'), ['source_id', 'target_id']
    ]
    .rename(columns={'source_id': 'model_id', 'target_id': 'material_id'})
)
composicao = composicao.merge(materiais, on='material_id', how='left', validate='many_to_one')
composicao['batch_id'] = batch_id

def presenca_material(serie):
    # Um True comprova presença; False exige todos os componentes conhecidos.
    if serie.fillna(False).any():
        return True
    return False if serie.notna().all() else pd.NA

resumo_materiais = composicao.groupby('model_id', dropna=False).agg(
    quantidade_materiais_associados=('material_id', 'size'),
    possui_material_reciclavel=('recyclable', presenca_material),
    possui_material_perigoso=('hazardous', presenca_material),
).reset_index()
itens = itens.merge(resumo_materiais, on='model_id', how='left', validate='many_to_one')
for coluna in ['possui_material_reciclavel', 'possui_material_perigoso']:
    itens[coluna] = itens[coluna].astype('boolean')
composicao = composicao.rename(columns={'model_id':'id_modelo','material_id':'id_material',
    'name':'nome_material','recyclable':'material_reciclavel','hazardous':'material_perigoso'})
salvar_delta(composicao, 'workspace.zera_silver.composicao_modelo_material')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Renomeação das colunas
# MAGIC **Problema encontrado:** as fontes usam convenções e idiomas diferentes, o que dificulta a leitura das tabelas finais e dos indicadores.
# MAGIC **Estratégia:** após usar os nomes originais nas junções, padronizar os campos analíticos em português e gravar um dicionário que relaciona cada nome tratado à coluna de origem.
# MAGIC **Resultado:** o dicionário torna a mudança rastreável; as tabelas de origem Silver preservam seus nomes técnicos para facilitar a auditoria.

# COMMAND ----------

# DBTITLE 1,6.1. Padronização dos nomes e dicionário de colunas
nomes_portugues = {
    'acquiredAt': 'data_aquisicao',
    'category_id': 'id_categoria',
    'category_name': 'categoria',
    'city': 'cidade',
    'condition': 'condicao',
    'createdBy': 'id_responsavel',
    'createdByName': 'responsavel',
    'createdAt': 'data_cadastro',
    'damages': 'danos',
    'estimatedWeightKg': 'peso_estimado_kg',
    'expectedLifespanMonths': 'vida_util_estimada_meses',
    'hasDamages': 'indicador_danos_origem',
    'id': 'id_item',
    'lastEventAt': 'data_ultimo_evento',
    'manufacturer': 'fabricante',
    'manufacturingYear': 'ano_fabricacao',
    'model_id': 'id_modelo',
    'model_name': 'modelo',
    'name': 'nome_item',
    'organization_id': 'id_organizacao',
    'organization_name': 'organizacao',
    'status': 'situacao',
    'state': 'uf',
    'unitId': 'id_unidade',
    'unit_name': 'unidade',
    'updatedAt': 'data_atualizacao',
    'usageIntensity': 'intensidade_uso',
    'warrantyMonths': 'garantia_meses',
}
itens = itens.rename(columns=nomes_portugues)
nomes_eventos = {
    'id': 'id_evento',
    'itemId': 'id_item',
    'unitId': 'id_unidade',
    'type': 'tipo_evento',
    'toStatus': 'situacao_destino',
    'occurredAt': 'data_ocorrencia',
}
nomes_composicao = {
    'model_id': 'id_modelo',
    'material_id': 'id_material',
    'name': 'nome_material',
    'recyclable': 'material_reciclavel',
    'hazardous': 'material_perigoso',
}
eventos = eventos.rename(columns=nomes_eventos)
salvar_delta(eventos, 'workspace.zera_silver.eventos')
dicionario = pd.concat([
    pd.DataFrame([
        ['itens_tratados', origem, destino]
        for origem, destino in nomes_portugues.items()
    ]),
    pd.DataFrame([
        ['eventos', origem, destino]
        for origem, destino in nomes_eventos.items()
    ]),
    pd.DataFrame([
        ['composicao_modelo_material', origem, destino]
        for origem, destino in nomes_composicao.items()
    ]),
], ignore_index=True)
dicionario.columns = ['tabela', 'coluna_original', 'coluna_tratada']
display(dicionario)
salvar_delta(dicionario, 'workspace.zera_silver.dicionario_colunas')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Qualidade, indicadores derivados e tempo
# MAGIC **Problema encontrado:** datas futuras, medidas inválidas e danos desconhecidos podem distorcer comparações ou ser interpretados como valores válidos.
# MAGIC **Estratégia:** preservar os valores de origem e criar sinalizadores explícitos para inconsistências; nenhuma correção é inferida automaticamente.
# MAGIC Intensidade de uso não recebe escala presumida; danos desconhecidos permanecem nulos.
# MAGIC Idade é calculada desde a aquisição até a referência do lote. A conversão mensal usa 365,25 / 12 dias.
# MAGIC Peso e vida útil são estimativas do modelo, não medidas de descarte nem prazos obrigatórios.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.1. Sinalizadores de qualidade
# MAGIC **Estratégia:** criar um indicador separado para cada condição revisável e combinar esses indicadores em um alerta geral, sem descartar a linha.

# COMMAND ----------

# DBTITLE 1,7.1. Sinalizadores de datas, valores e danos
itens['data_referencia'] = data_referencia
itens['alerta_unidade_ausente'] = ~itens['unidade_encontrada']
itens['alerta_modelo_ausente'] = ~itens['modelo_encontrado']
itens['alerta_aquisicao_futura'] = itens['data_aquisicao'].gt(data_referencia)
itens['alerta_atualizacao_anterior'] = itens['data_atualizacao'].lt(itens['data_cadastro'])
itens['alerta_peso_invalido'] = itens['peso_estimado_kg'].le(0)
itens['alerta_vida_util_invalida'] = itens['vida_util_estimada_meses'].le(0)
itens['alerta_ano_fabricacao_invalido'] = (
    itens['ano_fabricacao'].le(0)
    | itens['ano_fabricacao'].gt(data_referencia.year)
)
danos_lista = itens['danos'].map(
    lambda valor: (
        len(valor) > 0
        if isinstance(valor, (list, np.ndarray))
        else pd.NA
    )
).astype('boolean')
itens['possui_danos'] = itens['indicador_danos_origem'].fillna(danos_lista).astype('boolean')
itens['alerta_danos_divergentes'] = (
    itens['indicador_danos_origem'] != danos_lista
).astype('boolean')
colunas_alerta = [coluna for coluna in itens.columns if coluna.startswith('alerta_')]
itens['alerta_qualidade_dos_dados'] = itens[colunas_alerta].fillna(False).any(axis=1)
for coluna in colunas_alerta:
    verificacoes.append(('itens_tratados', coluna, int(itens[coluna].fillna(False).sum())))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.2. Indicadores temporais
# MAGIC **Estratégia:** calcular idade desde a aquisição até a referência do lote e manter valores não calculáveis quando a data ou a vida útil estimada não permite comparação.

# COMMAND ----------

# DBTITLE 1,7.2. Idade, vida útil estimada e faixas temporais
itens['ano_cadastro'] = itens['data_cadastro'].dt.year.astype('Int64')
itens['mes_cadastro'] = itens['data_cadastro'].dt.month.astype('Int64')
itens['mes_ano_cadastro'] = itens['data_cadastro'].dt.strftime('%Y-%m')
idade_dias = (data_referencia - itens['data_aquisicao'].dt.normalize()).dt.days
itens['idade_meses'] = (idade_dias / (365.25 / 12)).where(~itens['alerta_aquisicao_futura'])
vida_valida = itens['vida_util_estimada_meses'].gt(0)
itens['percentual_vida_util_consumida'] = (
    itens['idade_meses'] / itens['vida_util_estimada_meses'] * 100
).where(vida_valida)
itens['meses_restantes_estimados'] = (itens['vida_util_estimada_meses'] - itens['idade_meses']).where(vida_valida)
itens['ultrapassou_vida_util_estimada'] = itens['percentual_vida_util_consumida'].ge(100).astype('boolean')
itens.loc[itens['percentual_vida_util_consumida'].isna(), 'ultrapassou_vida_util_estimada'] = pd.NA
itens['faixa_idade'] = pd.cut(
    itens['idade_meses'],
    bins=[0, 12, 24, 48, np.inf],
    labels=[
        'Menos de 12 meses',
        '12 a menos de 24 meses',
        '24 a menos de 48 meses',
        '48 meses ou mais',
    ],
    right=False,
).astype('string').fillna('Não informada')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Validação final e publicação da Silver
# MAGIC **Problema encontrado:** integrações podem aumentar ou reduzir o número de itens sem que isso fique evidente em uma amostra.
# MAGIC **Estratégia:** reconciliar quantidade e unicidade de itens com a fonte Neo4j, registrar verificações de qualidade e publicar todas as tabelas com o manifesto do mesmo lote.

# COMMAND ----------

# DBTITLE 1,8. Validação final, manifesto e tabelas de saída
if len(itens) != len(bases['neo_item']) or not itens['id_item'].is_unique:
    raise ValueError('A integração alterou a quantidade ou a unicidade dos itens.')
verificacoes_df = pd.DataFrame(verificacoes, columns=['entidade','verificacao','ocorrencias'])
verificacoes_df['id_lote'] = batch_id
qualidade = verificacoes_df.copy()
salvar_delta(itens, 'workspace.zera_silver.itens_tratados')
salvar_delta(qualidade, 'workspace.zera_silver.qualidade')
salvar_delta(manifest, 'workspace.zera_silver.manifest')
display(qualidade.sort_values('ocorrencias', ascending=False))
display(itens[[
    'id_item', 'categoria', 'situacao', 'condicao', 'responsavel', 'idade_meses',
    'faixa_idade', 'alerta_qualidade_dos_dados',
]].head(20))
print('Base tratada:', len(itens), 'itens;', len(itens.columns), 'colunas.')
dbutils.notebook.exit(json.dumps({'batch_id':batch_id,'itens':len(itens),'eventos':len(eventos),
    'colunas_itens_tratados':len(itens.columns),'ocorrencias_qualidade':int(qualidade['ocorrencias'].sum())}))