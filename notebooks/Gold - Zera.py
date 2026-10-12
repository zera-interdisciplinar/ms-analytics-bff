# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Gold - Zera | Indicadores e bases analíticas
# MAGIC **Objetivo:** transformar as tabelas tratadas em KPIs e conjuntos prontos para análise e dashboard.
# MAGIC **Indicadores nesta versão:** itens cadastrados; distribuição por categoria e situação; danos registrados; modelos; alertas; eventos por item; potencial reciclável dos itens; preenchimento da classificação de reciclabilidade dos materiais; e taxa de alertas resolvidos.
# MAGIC **Problema analisado:** descrever o inventário cadastrado, sua composição e a cobertura dos registros operacionais sem extrapolar o que os dados permitem afirmar.
# MAGIC **Fonte e escopo:** tabelas Silver do mesmo lote, em ambiente de testes/QA. A tabela principal tem uma linha por item; um item pode estar associado a mais de um material.
# MAGIC **Método:** cada KPI documenta fórmula, finalidade, denominador e ressalvas. Agrupamentos, indicadores e gráficos são calculados com pandas; Spark é usado somente para acesso Delta.
# MAGIC **Referência de modelagem:** [documentação PostgreSQL do projeto](https://zerainterdisciplinar.atlassian.net/wiki/spaces/BDD/pages/54788097/4.+Modelagem+dimensional+Star+Snowflake+CTEs+e+window+functions). As views `bi` documentadas pertencem ao PostgreSQL. Este Gold lê a Silver no Databricks para manter a origem e o lote rastreáveis.
# MAGIC **View candidata:** `vw_alertas_mensal_unidade` parece relacionada ao tema do inventário. A Silver já traz os registros detalhados de alertas, então o Gold pode calcular essa série a partir da fonte tratada. Só faz sentido extrair a view para a Bronze se a definição SQL trouxer uma regra de negócio que ainda não esteja nas tabelas base; antes disso, ela deve ser comparada com os alertas Silver. `vw_ranking_unidades`, `vw_dau_diario` e `vw_hierarquia_equipe` tratam de pontuação, acessos e equipes, fora do escopo atual.
# MAGIC **Limites:** não há metas definidas, escala conhecida para intensidade de uso, nem interpretação presumida de estoque ativo. Mapas dependem de cobertura geográfica adequada.

# COMMAND ----------
# MAGIC %pip install "matplotlib>=3.8,<4"

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Preparação da análise
# MAGIC **Problema encontrado:** transferir a base inteira para pandas sem limite pode exceder a memória do driver; misturar funções de cálculo e escrita também dificulta a leitura do notebook.
# MAGIC **Estratégia:** limitar as leituras, fazer os cálculos com pandas e usar Spark somente para ler e gravar tabelas Delta.

# COMMAND ----------
# DBTITLE 1,1. Dependências, leitura e publicação Delta
import json
import base64
import io
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

    linhas = [tuple(valor_delta(v, c.dataType) for v, c in zip(linha, campos))
              for linha in df.itertuples(index=False, name=None)]
    dados_delta = spark.createDataFrame(linhas, T.StructType(campos))
    if spark.catalog.tableExists(tabela):
        spark.sql(f"ALTER TABLE {tabela} SET TBLPROPERTIES ('delta.feature.timestampNtz'='supported')")
    dados_delta.write.format('delta').mode(modo).option('overwriteSchema', 'true').saveAsTable(tabela)

spark.sql('CREATE SCHEMA IF NOT EXISTS workspace.zera_gold')

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
# MAGIC ## 2. Leitura e diagnóstico da Silver
# MAGIC **Problema encontrado:** juntar tabelas de lotes diferentes ou assumir que as colunas tratadas existem pode distorcer os indicadores ou interromper a análise.
# MAGIC **Estratégia:** selecionar o manifesto com um único lote, carregar as tabelas correspondentes, validar o `batch_id` e a unicidade da chave dos itens e apresentar uma amostra da base.
# MAGIC **O que cada linha representa:** um item na base principal; eventos e composição de materiais permanecem em tabelas separadas.
# MAGIC **Por que a Gold lê várias tabelas Silver:** o manifesto identifica o lote; itens alimentam os indicadores de inventário; eventos, alertas e relatórios alimentam suas próprias medidas; modelos, materiais e composição sustentam os indicadores de materiais; unidades e organizações dão contexto aos recortes; e as configurações de unidade servem apenas para uma análise candidata de capacidade.

# COMMAND ----------
# DBTITLE 1,2. Leitura da Silver e validação de consistência do lote
manifest = ler_delta('workspace.zera_silver.manifest')
if manifest['batch_id'].nunique() != 1:
    raise ValueError('Manifesto Silver deve ter um único lote.')
batch_id = manifest['batch_id'].iloc[0]
itens = ler_delta('workspace.zera_silver.itens_tratados').rename(columns={
    'situacao':'status','indicador_danos_origem':'hasDamages','danos':'damages',
    'alerta_qualidade_dos_dados':'flag_atencao_dados'})
eventos = ler_delta('workspace.zera_silver.eventos').rename(columns={
    'data_ocorrencia':'occurred_at','id_unidade':'unitId','tipo_evento':'type',
    'situacao_destino':'toStatus','id_item':'itemId'})
relatorios = ler_delta('workspace.zera_silver.pg_disposal_report')
alertas = ler_delta('workspace.zera_silver.pg_alert')
relacoes_material = ler_delta('workspace.zera_silver.neo_relationships')
materiais_pipeline = ler_delta('workspace.zera_silver.neo_material').rename(columns={
    'id':'material_id','name':'name','recyclable':'recyclable','hazardous':'hazardous'})
composicao = (
    relacoes_material.loc[
        relacoes_material['relationship_type'].eq('MADE_OF'),
        ['batch_id','extracted_at','source_id','target_id'],
    ]
    .rename(columns={'source_id':'id_modelo','target_id':'material_id'})
    .merge(
        materiais_pipeline[['batch_id','material_id','name','recyclable','hazardous']],
        on=['batch_id','material_id'], how='left', validate='many_to_one',
    )
)
cadastros = {
    'pg_unit': ler_delta('workspace.zera_silver.pg_unit'),
    'pg_organization': ler_delta('workspace.zera_silver.pg_organization'),
    'neo_material': ler_delta('workspace.zera_silver.neo_material'),
    'neo_model': ler_delta('workspace.zera_silver.neo_model'),
}
configuracoes_unidade = ler_delta('workspace.zera_silver.neo_unitinventorysettings')
itens['percentual_vida_util_consumida'] = (
    itens['idade_meses'] / itens['vida_util_estimada_meses'] * 100
).where(itens['vida_util_estimada_meses'].gt(0))
itens['mes_ano_cadastro'] = pd.to_datetime(
    itens['data_cadastro'], errors='coerce'
).dt.strftime('%Y-%m')
itens['faixa_idade'] = pd.cut(
    itens['idade_meses'],
    bins=[0, 12, 24, 48, np.inf],
    labels=['Menos de 12 meses', '12 a menos de 24 meses',
            '24 a menos de 48 meses', '48 meses ou mais'],
    right=False,
).astype('string').fillna('Não informada')
for df in [
    itens, eventos, relatorios, alertas, composicao,
    configuracoes_unidade, *cadastros.values(),
]:
    if not df.empty and not df['batch_id'].eq(batch_id).all():
        raise ValueError('As tabelas Silver pertencem a lotes diferentes.')
if not itens['id_item'].is_unique:
    raise ValueError('IDs de item repetidos na Silver.')
data_referencia = pd.Timestamp(manifest['extracted_at'].iloc[0]).date()
total_itens = len(itens)
print('Base QA | Referência:', data_referencia, '| Itens:', total_itens)
display(itens[['categoria','modelo','status','condicao','idade_meses','peso_estimado_kg','responsavel']].head(20))
perfil_itens_gold = pd.DataFrame({
    'coluna': itens.columns,
    'tipo': [str(itens[coluna].dtype) for coluna in itens.columns],
    'nulos': [int(itens[coluna].isna().sum()) for coluna in itens.columns],
    'percentual_nulos': [
        itens[coluna].isna().mean() * 100 if len(itens) else np.nan
        for coluna in itens.columns
    ],
})
print('Diagnóstico inicial da tabela de itens recebida da Silver:')
print('Linhas:', len(itens), '| Colunas:', len(itens.columns))
print('Linhas completamente duplicadas:', contar_linhas_duplicadas(itens))
display(perfil_itens_gold.sort_values('percentual_nulos', ascending=False))

def publicar(nome, df):
    df = df.copy()
    df['batch_id'] = batch_id
    df['published_at'] = pd.Timestamp.now(tz='UTC').tz_localize(None)
    salvar_delta(df, 'workspace.zera_gold.' + nome)

def percentual(numerador, denominador):
    return numerador / denominador * 100 if denominador else None

indicadores = []
def registrar_kpi(
    nome, valor, unidade, denominador, formula, importancia,
    ressalva='', recorte=None,
):
    valor = None if valor is None or pd.isna(valor) else float(valor)
    indicadores.append([
        nome, valor, unidade, denominador, formula,
        importancia, ressalva, recorte,
    ])
    print(nome + ':', 'Não calculável' if valor is None else round(valor, 3), unidade,
          '| Base de cálculo:', denominador)

# A documentação confirma OPEN como estado aberto e descreve uma rotina que fecha
# alertas obsoletos; ela não informa o literal gravado no estado final. Como o lote
# QA não contém alertas, só inclua valores aqui após confirmar o enum com a aplicação.
STATUS_ALERTA_RESOLVIDO = set()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Indicadores e métricas
# MAGIC **Problema encontrado:** contagens sem denominador ou sem ressalvas podem ser interpretadas como desempenho operacional, mesmo quando os dados são apenas de QA.
# MAGIC **Estratégia:** calcular cada indicador no nível de detalhe apropriado, declarar a fórmula, a base de cálculo e as limitações, e deixar o resultado não calculável quando faltar uma regra de negócio.

# COMMAND ----------
# MAGIC ### 3.1. KPI — Itens cadastrados
# MAGIC **Fórmula:** contagem de IDs distintos de item. **Importância:** dimensionar o inventário cadastrado; não equivale a estoque ativo.


# COMMAND ----------
# DBTITLE 1,3.1. Cálculo do KPI | Itens cadastrados
registrar_kpi(
    'itens_cadastrados', total_itens, 'itens', total_itens,
    'IDs distintos', 'Dimensionar o cadastro',
    'Inclui DRAFT; não equivale a estoque ativo.',
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3.2. KPI — Itens por categoria
# MAGIC **Fórmula:** contagem de itens em cada categoria. Categorias ausentes aparecem como “Não informada”.


# COMMAND ----------
# DBTITLE 1,3.2. Distribuição dos itens por categoria
por_categoria = (
    itens.assign(categoria=itens['categoria'].fillna('Não informada'))
    .groupby('categoria', dropna=False)
    .agg(quantidade_itens=('id_item', 'nunique'))
    .reset_index()
)
por_categoria['percentual_itens'] = por_categoria['quantidade_itens'].map(
    lambda quantidade: percentual(quantidade, total_itens)
)
publicar('itens_por_categoria', por_categoria)
for linha in por_categoria.itertuples(index=False):
    registrar_kpi(
        'itens_por_categoria', linha.quantidade_itens, 'itens', total_itens,
        'Contagem de IDs distintos por categoria',
        'Comparar a composição do inventário', recorte=linha.categoria,
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3.3. KPI — Itens por situação
# MAGIC **Fórmula:** contagem de itens em cada situação registrada. Situações ausentes são preservadas como “Não informada”.


# COMMAND ----------
# DBTITLE 1,3.3. Distribuição dos itens por situação
por_situacao = (
    itens.assign(status=itens['status'].fillna('Não informada'))
    .groupby('status', dropna=False)
    .agg(quantidade_itens=('id_item', 'nunique'))
    .reset_index()
    .rename(columns={'status': 'situacao'})
)
por_situacao['percentual_itens'] = por_situacao['quantidade_itens'].map(
    lambda quantidade: percentual(quantidade, total_itens)
)
publicar('itens_por_situacao', por_situacao)
for linha in por_situacao.itertuples(index=False):
    registrar_kpi(
        'itens_por_situacao', linha.quantidade_itens, 'itens', total_itens,
        'Contagem de IDs distintos por situação',
        'Acompanhar a composição do inventário', recorte=linha.situacao,
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3.4. KPI — Itens com danos registrados
# MAGIC **Fórmula:** contagem de itens cuja indicação de danos é verdadeira. Registros sem informação não são tratados como “sem danos”.


# COMMAND ----------
# DBTITLE 1,Itens com danos registrados
danos_conhecidos = itens['possui_danos'].dropna()
itens_com_danos = int(danos_conhecidos.astype(bool).sum())
registrar_kpi(
    'itens_com_danos', itens_com_danos, 'itens', len(danos_conhecidos),
    'Contagem de itens com indicador de danos verdadeiro',
    'Dimensionar os itens com danos informados',
    'O denominador inclui apenas itens com informação conhecida.',
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3.5. KPI — Modelos cadastrados
# MAGIC **Fórmula:** contagem de IDs distintos de modelo.


# COMMAND ----------
# DBTITLE 1,3.5. Cálculo do KPI | Modelos cadastrados
total_modelos = cadastros['neo_model']['id'].nunique()
registrar_kpi(
    'modelos_cadastrados', total_modelos, 'modelos', total_modelos,
    'Contagem de IDs distintos', 'Dimensionar o catálogo de modelos',
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3.6. KPI — Alertas registrados
# MAGIC **Fórmula:** contagem de alertas no lote.


# COMMAND ----------
# DBTITLE 1,3.6. Cálculo do KPI | Alertas registrados
registrar_kpi(
    'alertas_registrados', len(alertas), 'alertas', len(alertas),
    'Contagem de IDs de alerta', 'Acompanhar os alertas gerados',
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3.7. KPI — Eventos por item
# MAGIC **Fórmula:** eventos registrados ÷ itens cadastrados. Não é uma taxa mensal e inclui eventos de criação.


# COMMAND ----------
# DBTITLE 1,Eventos por item
registrar_kpi(
    'eventos_por_item', len(eventos) / total_itens if total_itens else None,
    'eventos/item', total_itens, 'Total de eventos / total de itens',
    'Dimensionar o histórico médio registrado por item',
    'Não é uma taxa mensal; inclui eventos de criação.',
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3.8. KPI — Potencial reciclável dos itens
# MAGIC **Fórmula:** itens com ao menos um material reciclável ÷ itens com classificação conhecida × 100. Não mede reciclagem efetiva nem massa.


# COMMAND ----------
# DBTITLE 1,Potencial reciclável dos itens
registrar_kpi(
    'potencial_reciclavel_itens_pct',
    percentual(
        int(itens.loc[itens['possui_material_reciclavel'].notna(),
                      'possui_material_reciclavel'].sum()),
        int(itens['possui_material_reciclavel'].notna().sum()),
    ),
    '%', int(itens['possui_material_reciclavel'].notna().sum()),
    'Itens com potencial reciclável / itens com classificação conhecida × 100',
    'Descrever o potencial da composição cadastrada',
    'Não comprova reciclagem nem representa percentual de massa.',
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3.9. KPI — Classificação de reciclabilidade dos materiais preenchida
# MAGIC **Fórmula:** materiais com classificação conhecida ÷ total de materiais × 100.


# COMMAND ----------
# DBTITLE 1,Classificação de reciclabilidade dos materiais
classificacao_material = cadastros['neo_material']['recyclable']
materiais_classificados = int(classificacao_material.notna().sum())
registrar_kpi(
    'materiais_com_classificacao_reciclabilidade_pct',
    percentual(materiais_classificados, len(classificacao_material)), '%',
    len(classificacao_material), 'Classificados / total de materiais × 100',
    'Avaliar cobertura da informação ambiental do catálogo',
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3.10. KPI — Taxa de alertas resolvidos
# MAGIC **Fórmula:** alertas com situação de resolução confirmada ÷ alertas com situação conhecida × 100. Os valores de situação considerados resolvidos aguardam confirmação.


# COMMAND ----------
# DBTITLE 1,Taxa de alertas resolvidos
alertas_com_situacao = alertas['status'].dropna().astype('string').str.upper()
denominador_alertas = len(alertas_com_situacao)
alertas_resolvidos = int(
    alertas_com_situacao.isin(STATUS_ALERTA_RESOLVIDO).sum()
)
taxa_alertas_resolvidos = (
    percentual(alertas_resolvidos, denominador_alertas)
    if STATUS_ALERTA_RESOLVIDO
    else None
)
ressalva_status_alerta = (
    'Confirmar os valores de status que significam alerta resolvido.'
    if not STATUS_ALERTA_RESOLVIDO
    else 'Sem alertas com status conhecido: não calculável.'
)
registrar_kpi(
    'taxa_alertas_resolvidos_pct', taxa_alertas_resolvidos, '%',
    denominador_alertas, 'Alertas resolvidos / alertas com situação conhecida × 100',
    'Acompanhar o tratamento dos alertas', ressalva_status_alerta,
)

# COMMAND ----------
# DBTITLE 1,3.11. Publicação dos KPIs, bases de análise e cobertura
kpis = pd.DataFrame(
    indicadores,
    columns=[
        'indicador', 'valor', 'unidade', 'denominador',
        'formula', 'importancia', 'ressalva', 'recorte',
    ],
)
publicar('kpis_analiticos', kpis)
contagens = {
    'itens_cadastrados': total_itens,
    'eventos_registrados': len(eventos),
    'unidades_cadastradas': len(cadastros['pg_unit']),
    'organizacoes_cadastradas': len(cadastros['pg_organization']),
    'materiais_cadastrados': len(cadastros['neo_material']),
    'modelos_cadastrados': len(cadastros['neo_model']),
    'unidades_com_inventario': int(itens['id_unidade'].dropna().nunique()),
    'relatorios_descarte_registrados': len(relatorios),
    'alertas_registrados': len(alertas),
}
publicar('kpis_gerais', pd.DataFrame(contagens.items(), columns=['indicador','valor']))
publicar('itens_analise', itens)
publicar('itens_dashboard', itens)
display(kpis[['indicador','valor','unidade','denominador','ressalva']])

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Comparações e dimensões para filtros
# MAGIC **Escopo dos KPIs:** os dez indicadores selecionados estão na tabela `kpis_analiticos`. As tabelas abaixo dão suporte a filtros e explorações; não acrescentam KPIs à lista acordada.
# MAGIC **Objetivo:** disponibilizar recortes por situação, unidade, organização, categoria, condição, idade e material.
# MAGIC **Tratamento:** categorias ausentes são preservadas como grupo, e materiais são contados como associações distintas. Como um item pode ter mais de um material, grupos de materiais podem se sobrepor.
# MAGIC **Série temporal:** a série de cadastros usa calendário diário completo entre a primeira e a última data observadas e inclui dias sem cadastro com zero. Unidades ausentes ficam agrupadas como “Não informada”. A variação mensal só compara meses consecutivos; o mês mais recente pode estar incompleto.
# MAGIC **Novas bases candidatas:** eventos por mês e alertas por situação/gravidade. Unidades com inventário entram no resumo geral. A ocupação usa `stockCapacity` apenas como campo candidato até confirmarmos sua definição de negócio.
# MAGIC **Verificação de cardinalidade:** os recortes são agregados por item antes da publicação para não multiplicar itens ao relacionar materiais ou configurações por unidade.

# COMMAND ----------
# DBTITLE 1,4.1. Agregações do inventário e composição
por_status = por_situacao.rename(columns={
    'situacao': 'status', 'percentual_itens': 'percentual',
})
publicar('itens_por_status', por_status)
grupos = [
    'id_unidade', 'unidade', 'organizacao', 'cidade', 'uf',
    'categoria', 'status', 'condicao',
]
perfil = (
    itens.groupby(grupos, dropna=False)
    .agg(
        quantidade_itens=('id_item', 'size'),
        idade_media_meses=('idade_meses', 'mean'),
    )
    .reset_index()
)
publicar('perfil_inventario', perfil)
publicar('itens_por_unidade_categoria', perfil)
itens_por_unidade = (
    itens.dropna(subset=['id_unidade'])
    .groupby(['id_unidade', 'unidade'], dropna=False)
    .agg(quantidade_itens=('id_item', 'nunique'))
    .reset_index()
)
publicar('unidades_com_inventario', itens_por_unidade)

# `stockCapacity` é publicado para validar sua definição antes de tratá-lo como meta.
if configuracoes_unidade['unitId'].duplicated().any():
    raise ValueError('Há mais de uma configuração de capacidade por unidade.')
capacidade_unidade = configuracoes_unidade[
    ['unitId', 'stockCapacity']
].rename(columns={
    'unitId': 'id_unidade',
    'stockCapacity': 'capacidade_cadastrada',
})
ocupacao_por_unidade = itens_por_unidade.merge(
    capacidade_unidade,
    on='id_unidade',
    how='left',
    validate='one_to_one',
)
capacidade_positiva = ocupacao_por_unidade['capacidade_cadastrada'].gt(0)
ocupacao_por_unidade['ocupacao_capacidade_pct'] = (
    ocupacao_por_unidade['quantidade_itens']
    .div(ocupacao_por_unidade['capacidade_cadastrada'])
    .mul(100)
    .where(capacidade_positiva)
)
ocupacao_por_unidade['definicao_validada'] = False
publicar('ocupacao_capacidade_unidade_candidata', ocupacao_por_unidade)

distribuicao = (
    itens.groupby(['categoria', 'faixa_idade'], dropna=False)
    .size()
    .reset_index(name='quantidade_itens')
)
publicar('distribuicao_idade', distribuicao)
por_material = itens[['id_item', 'id_modelo']].merge(
    composicao,
    on='id_modelo',
    how='inner',
)
por_material = (
    por_material.groupby(
        ['material_id', 'name', 'recyclable', 'hazardous'],
        dropna=False,
    )['id_item']
    .nunique()
    .reset_index(name='itens_associados')
)
publicar('itens_por_material', por_material)

# COMMAND ----------
# DBTITLE 1,4.2. Evolução diária e mensal dos cadastros
datas_cadastro = pd.to_datetime(itens['data_cadastro'], errors='coerce').dt.normalize()
datas_validas = datas_cadastro.dropna()
itens_calendario = itens.assign(
    id_unidade=itens['id_unidade'].fillna('Não informada')
)
unidades_calendario = pd.concat([
    cadastros['pg_unit']['id'], itens_calendario['id_unidade'],
], ignore_index=True).fillna('Não informada').drop_duplicates().tolist()

if datas_validas.empty or not unidades_calendario:
    diario = pd.DataFrame(columns=['data', 'id_unidade', 'cadastros'])
else:
    calendario = pd.date_range(datas_validas.min(), datas_validas.max(), freq='D')
    linhas_calendario = len(calendario) * len(unidades_calendario)
    if linhas_calendario > limite_linhas:
        raise ValueError('O calendário diário excedeu o limite de linhas desta análise pandas.')
    grade_diaria = pd.MultiIndex.from_product(
        [calendario, unidades_calendario], names=['data', 'id_unidade']
    ).to_frame(index=False)
    cadastros_diarios_observados = (
        itens_calendario.assign(data=datas_cadastro)
        .dropna(subset=['data'])
        .groupby(['data', 'id_unidade'], dropna=False)
        .size()
        .reset_index(name='cadastros')
    )
    diario = grade_diaria.merge(
        cadastros_diarios_observados,
        on=['data', 'id_unidade'], how='left', validate='one_to_one',
    )
    diario['cadastros'] = diario['cadastros'].fillna(0).astype('int64')
    diario['data'] = diario['data'].dt.date
publicar('evolucao_cadastros', diario)
publicar('cadastros_diarios', diario)
if diario.empty:
    mensal = pd.DataFrame(columns=[
        'mes_ano_cadastro', 'id_unidade', 'cadastros',
        'mes_anterior_observado', 'cadastros_mes_anterior', 'crescimento_pct',
    ])
else:
    mensal = diario.assign(
        mes_ano=pd.to_datetime(diario['data']).dt.strftime('%Y-%m')
    ).groupby(
        ['mes_ano', 'id_unidade'], dropna=False
    )['cadastros'].sum().reset_index()
    mensal = mensal.rename(columns={'mes_ano': 'mes_ano_cadastro'})
    mensal = mensal.sort_values(['id_unidade', 'mes_ano_cadastro'])
    mensal['mes_anterior_observado'] = mensal.groupby(
        'id_unidade', dropna=False
    )['mes_ano_cadastro'].shift()
    mensal['cadastros_mes_anterior'] = mensal.groupby(
        'id_unidade', dropna=False
    )['cadastros'].shift()
    mes_atual = pd.to_datetime(mensal['mes_ano_cadastro'], format='%Y-%m')
    mes_anterior = pd.to_datetime(mensal['mes_anterior_observado'], format='%Y-%m')
    consecutivos = (mes_atual - pd.DateOffset(months=1)).eq(mes_anterior)
    mensal['crescimento_pct'] = (
        (mensal['cadastros'] / mensal['cadastros_mes_anterior'] - 1) * 100
    ).where(consecutivos & mensal['cadastros_mes_anterior'].gt(0))
publicar('cadastros_mensais', mensal)
eventos['data'] = pd.to_datetime(eventos['occurred_at']).dt.date
eventos_diarios = (
    eventos.groupby(['data','unitId','type'], dropna=False)
    .size().reset_index(name='quantidade_eventos')
)
publicar('eventos_diarios', eventos_diarios)
eventos['mes'] = pd.to_datetime(eventos['occurred_at']).dt.strftime('%Y-%m')
eventos_mensais = (
    eventos.groupby(['mes', 'type', 'unitId'], dropna=False)
    .size()
    .reset_index(name='quantidade_eventos')
    .sort_values(['mes', 'type', 'unitId'])
)
publicar('eventos_mensais', eventos_mensais)
alertas_por_status = (
    alertas.groupby(['status', 'unit_id'], dropna=False)
    .size()
    .reset_index(name='quantidade_alertas')
)
publicar('alertas_por_status', alertas_por_status)
alertas_por_gravidade = (
    alertas.groupby(['severity', 'unit_id'], dropna=False)
    .size()
    .reset_index(name='quantidade_alertas')
)
publicar('alertas_por_gravidade', alertas_por_gravidade)
publicar(
    'alertas_por_status_e_gravidade',
    alertas.groupby(
        ['status', 'severity', 'kind', 'unit_id'], dropna=False
    )
    .size()
    .reset_index(name='quantidade_alertas'),
)
relatorios_por_status = (
    relatorios.groupby(['status', 'unit_id'], dropna=False)
    .size()
    .reset_index(name='quantidade_relatorios')
)
publicar('relatorios_descarte_por_status', relatorios_por_status)
for nome_base, base in [
    ('Eventos por mês', eventos_mensais),
    ('Alertas por situação', alertas_por_status),
    ('Alertas por gravidade', alertas_por_gravidade),
    ('Ocupação candidata por unidade', ocupacao_por_unidade),
]:
    print(f'{nome_base}: {len(base)} linhas')
    if base.empty:
        print('Sem registros disponíveis neste lote QA.')
    else:
        display(base)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Apresentação visual dos resultados
# MAGIC **Problema encontrado:** gráficos sem escala, período ou indicação de cobertura podem sugerir tendências que o lote não sustenta.
# MAGIC **Estratégia:** usar títulos, unidades e fonte consistentes; limitar gráficos de distribuição a uma amostra determinística e não apresentar evolução quando há menos de duas datas observadas.

# COMMAND ----------
# DBTITLE 1,Padrão visual das figuras
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
plt.rcParams.update({
    'font.family': 'DejaVu Sans',
    'font.size': 11,
    'figure.figsize': (10, 5),
    'axes.spines.top': False,
    'axes.spines.right': False,
})
figuras = {}
def exibir_grafico(figura, nome):
    figura.text(
        0.01, 0.01,
        f'Fonte: Silver Zera | QA | Referência: {data_referencia}',
        fontsize=9, color='#4b5563',
    )
    figura.tight_layout(rect=(0, 0.05, 1, 1))
    imagem = io.BytesIO()
    figura.savefig(imagem, format='png', dpi=140, bbox_inches='tight')
    figuras[nome] = base64.b64encode(imagem.getvalue()).decode('ascii')
    display(figura)
    plt.close(figura)

# COMMAND ----------
# DBTITLE 1,5.1. Gráfico de barras | Itens por situação
if not por_status.empty:
    dados = por_status.sort_values('quantidade_itens')
    fig, ax = plt.subplots()
    ax.barh(
        dados['status'].fillna('Não informado'),
        dados['quantidade_itens'],
        color='#2563eb',
    )
    ax.set(
        title='Itens cadastrados por status',
        xlabel='Quantidade de itens',
        xlim=(0, dados['quantidade_itens'].max() * 1.15),
    )
    for posicao, quantidade in enumerate(dados['quantidade_itens']):
        ax.text(quantidade, posicao, str(quantidade), va='center')
    exibir_grafico(fig, 'itens_por_status')

# COMMAND ----------
# DBTITLE 1,5.2. Gráfico de linha | Evolução dos cadastros
tendencia = (
    diario.groupby('data', dropna=False)['cadastros']
    .sum()
    .reset_index()
    .dropna(subset=['data'])
    .sort_values('data')
)
if not tendencia.empty:
    fig, ax = plt.subplots()
    ax.plot(tendencia['data'].astype(str), tendencia['cadastros'], marker='o', color='#2563eb')
    titulo = 'Cadastros nas datas observadas'
    if len(tendencia) == 1:
        titulo += ' — somente uma data'
    ax.set(
        title=titulo,
        xlabel='Data de cadastro',
        ylabel='Itens cadastrados',
        ylim=(0, None),
    )
    ax.grid(axis='y', alpha=0.2)
    exibir_grafico(fig, 'cadastros_observados')
print('Datas observadas:', len(tendencia), '| Uma data não demonstra evolução.')

# COMMAND ----------
# DBTITLE 1,5.3. Histograma | Distribuição da idade
amostra = itens.sort_values('id_item').head(5000)
idades = amostra.dropna(subset=['idade_meses'])
if not idades.empty:
    fig, ax = plt.subplots()
    ax.hist(idades['idade_meses'], bins='auto', color='#2563eb', edgecolor='white')
    ax.set(
        title=f'Distribuição da idade desde aquisição (n={len(idades)})',
        xlabel='Idade (meses)',
        ylabel='Quantidade de itens',
    )
    exibir_grafico(fig, 'histograma_idade')
print('Distribuições: até 5.000 itens selecionados por ID.')

# COMMAND ----------
# DBTITLE 1,5.4. Boxplot | Idade por categoria
if not idades.empty:
    dados = idades.copy()
    dados['categoria'] = dados['categoria'].fillna('Não informada')
    grupos = list(dados.groupby('categoria')['idade_meses'])
    fig, ax = plt.subplots()
    ax.boxplot([serie for nome, serie in grupos], tick_labels=[nome for nome, serie in grupos],
               patch_artist=True, boxprops={'facecolor':'#bfdbfe'}, medianprops={'color':'#1e3a8a'})
    ax.set(title=f'Idade por categoria (n={len(dados)})', ylabel='Idade (meses)')
    exibir_grafico(fig, 'boxplot_idade')

# COMMAND ----------
# DBTITLE 1,5.5. Gráfico de barras | Itens associados aos materiais
if not por_material.empty:
    dados = por_material.nlargest(20, 'itens_associados').sort_values('itens_associados')
    fig, ax = plt.subplots()
    ax.barh(dados['name'].fillna('Não informado'), dados['itens_associados'], color='#2563eb')
    ax.set(
        title='Itens associados a cada material — categorias sobrepostas',
        xlabel='Itens distintos associados',
        xlim=(0, None),
    )
    exibir_grafico(fig, 'materiais_associados')

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Leitura gerencial e validação dos resultados
# MAGIC **Validação:** reconciliar os cadastros temporais com a base de itens e explicitar os casos sem informação suficiente para concluir.
# MAGIC Os resultados abaixo são calculados nesta execução. São registros QA, não desempenho de produção.
# MAGIC Mapas dependem de geografia preenchida e verificável. Metas e desvios ainda precisam de definição.
# MAGIC O potencial reciclável descreve a composição cadastrada, não reciclagem realizada. A taxa de alertas resolvidos fica sem cálculo até confirmarmos quais valores de `status` significam resolução.

# COMMAND ----------
# DBTITLE 1,6.1. Reconciliação da base e ressalvas gerenciais
if int(diario['cadastros'].sum()) != total_itens:
    raise ValueError('As contagens temporais não reconciliam com os itens.')
conclusoes = [f'Cadastro QA: {total_itens} itens, {len(eventos)} eventos, {len(cadastros["neo_material"])} materiais.',
    f'Idade calculável em {len(idades)} de {total_itens} itens.',
    f'Relatórios de descarte: {len(relatorios)}; alertas: {len(alertas)}.']
if len(tendencia) < 2:
    conclusoes.append('Cobertura temporal insuficiente para concluir tendência de cadastros.')
if relatorios.empty:
    conclusoes.append('Sem relatórios de descarte: taxa de confirmação não calculável.')
if not STATUS_ALERTA_RESOLVIDO:
    conclusoes.append('Taxa de alertas resolvidos pendente: falta confirmar quais valores de status contam como resolvidos.')
if materiais_classificados < len(classificacao_material):
    conclusoes.append(f'Classificação de reciclabilidade preenchida em {materiais_classificados} de {len(classificacao_material)} materiais.')
for conclusao in conclusoes:
    print('•', conclusao)
publicar('conclusoes_gerenciais', pd.DataFrame({'ordem':range(1,len(conclusoes)+1),'conclusao':conclusoes}))
salvar_delta(manifest, 'workspace.zera_gold.manifest')
resultado_kpis = json.loads(kpis[['indicador','valor','unidade','denominador','ressalva','recorte']].to_json(orient='records', force_ascii=False))
dbutils.notebook.exit(json.dumps({'batch_id':batch_id,'kpis':contagens,'metricas':resultado_kpis,
    'conclusoes':conclusoes,'figuras_png':figuras}, ensure_ascii=False))
