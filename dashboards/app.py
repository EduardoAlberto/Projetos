from __future__ import annotations

from pathlib import Path

import streamlit as st
import plotly.express as px
import pandas as pd

from src.db import get_pool, read_query
from src.queries import (
    AGES,
    COUNTRIES,
    DEMANDS,
    GENDER,
    HOUSING,
    MIGRATION,
    SUMMARY,
    SILVER_COUNT,
    SILVER_CATALOG,
    SILVER_QUALITY,
)

st.set_page_config(
    page_title='CRAI | Dashboards',
    page_icon='📊',
    layout='wide',
    initial_sidebar_state='expanded',
)

style_path = Path(__file__).parent / '.streamlit' / 'style.css'
st.markdown(f'<style>{style_path.read_text(encoding="utf-8")}</style>', unsafe_allow_html=True)

COLORS = ['#087f73', '#e66b52', '#d99b35', '#24445c', '#62a89f', '#c54f39']


def format_number(value: int) -> str:
    return f'{int(value):,}'.replace(',', '.')


def quote_pg_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


@st.cache_resource
def get_connection_pool():
    return get_pool()


@st.cache_data(ttl=300, show_spinner=False)
def load_data(query: str):
    return read_query(get_connection_pool(), query)
  

def horizontal_chart(data, title, color):
    chart = px.bar(
        data.sort_values('total'),
        x='total',
        y='categoria',
        orientation='h',
        title=title,
        text='total',
        color_discrete_sequence=[color],
    )
    chart.update_traces(hovertemplate='%{y}<br>%{x:,} registros<extra></extra>', texttemplate='%{text:,}', textposition='outside')
    chart.update_layout(
        height=390,
        margin=dict(l=10, r=55, t=48, b=35),
        xaxis_title=None,
        yaxis_title=None,
        showlegend=False,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
    )
    return chart


with st.sidebar:
    st.header('Atualização')
    if st.button('Atualizar dados', use_container_width=True, type='primary'):
        st.cache_data.clear()
        st.rerun()
    st.caption('Os resultados ficam em cache por 5 minutos para preservar o banco durante a navegação.')

section = st.radio(
    'Navegação',
    ['Panorama dos atendimentos', 'Consolidado da camada Silver'],
    index=0,
    horizontal=True,
    label_visibility='collapsed',
)

if section == 'Panorama dos atendimentos':
    st.markdown('<p class="eyebrow">BASE CRAI 2014 A 2024</p>', unsafe_allow_html=True)
    st.title('Panorama dos atendimentos')
    st.caption('Dashboard conectado ao PostgreSQL | fonte: silver.base_crai_2014_2024_t3')

    try:
        summary = load_data(SUMMARY).iloc[0]
        demands = load_data(DEMANDS)
        countries = load_data(COUNTRIES)
        ages = load_data(AGES)
        housing = load_data(HOUSING)
        migration = load_data(MIGRATION)
        gender = load_data(GENDER)
    except Exception:
        st.error('Não foi possível consultar o PostgreSQL. Verifique as variáveis do arquivo .env e a disponibilidade do banco.')
        st.stop()

    metric_columns = st.columns(4)
    metric_columns[0].metric('Registros', format_number(summary['total_registros']))
    metric_columns[1].metric('Regularização migratória', format_number(summary['regularizacao_migratoria']))
    metric_columns[2].metric('País não informado', format_number(summary['pais_nao_informado']))
    metric_columns[3].metric('Escolaridade não informada', format_number(summary['escolaridade_nao_informada']))

    st.divider()
    first_row = st.columns(2)
    with first_row[0]:
        st.plotly_chart(horizontal_chart(demands, 'Principais demandas', COLORS[0]), use_container_width=True)
    with first_row[1]:
        st.plotly_chart(horizontal_chart(countries, 'Países de origem', COLORS[1]), use_container_width=True)

    second_row = st.columns(2)
    with second_row[0]:
        st.plotly_chart(horizontal_chart(ages, 'Faixa etária', COLORS[3]), use_container_width=True)
    with second_row[1]:
        st.plotly_chart(horizontal_chart(housing, 'Condição de moradia', COLORS[2]), use_container_width=True)

    third_row = st.columns(2)
    with third_row[0]:
        st.plotly_chart(horizontal_chart(migration, 'Situação migratória', COLORS[4]), use_container_width=True)
    with third_row[1]:
        st.plotly_chart(horizontal_chart(gender, 'Distribuição por sexo', COLORS[5]), use_container_width=True)

    st.info('Os países são agrupados ignorando maiúsculas, minúsculas e espaços externos. A tabela Silver ainda contém variações de acentuação e nomenclatura.')
else:
    st.markdown('<p class="eyebrow">CATÁLOGO | CAMADA SILVER</p>', unsafe_allow_html=True)
    st.title('Visão consolidada das tabelas Silver')
    st.caption('Inventário e volume por tabela, sem cruzar fontes que não possuem chaves comuns verificadas.')

    try:
        silver_catalog = load_data(SILVER_CATALOG)
        silver_quality = load_data(SILVER_QUALITY)
    except Exception:
        st.error('Não foi possível consultar o catálogo Silver. Verifique a conexão e o schema no PostgreSQL.')
        st.stop()

    known_tables = {
        'base_cadunico_imigrantes_2020_t2': 'Cadastro Único: imigrantes, 2020',
        'base_crai_2014_2024_t3': 'Atendimentos CRAI, 2014-2024',
        'base_crai_2025_t8': 'Atendimentos CRAI, 2025+',
        'dicionario_cadunico_imigrantes_2020_t6': 'Dicionário do Cadastro Único',
        'dicionario_crai_2014_2019_t5': 'Dicionário CRAI, 2014-2019',
        'dicionario_crai_2025_t1': 'Dicionário CRAI, 2025+',
        'dicionario_crai_2025_t4': 'Dicionário CRAI, 2025+ (cópia)',
        'programa_bolsa_familia_2025_t7': 'Programa Bolsa Família, 2025',
    }
    available_tables = set(silver_catalog['table_name'])
    inventory = []
    for table_name, descricao in known_tables.items():
        qualified_name = f'silver.{table_name}'
        if table_name in available_tables:
            count_query = f'SELECT COUNT(*) AS total FROM silver.{quote_pg_identifier(table_name)}'
            count = int(load_data(count_query).iloc[0]['total'])
            columns = silver_catalog.loc[
                silver_catalog['table_name'] == table_name, 'columns'
            ].iloc[0]
            state = 'Disponível'
        else:
            count = None
            columns = 'Aguardando materialização pelo pipeline'
            state = 'Pendente'
        inventory.append({
            'Tabela': qualified_name,
            'Conteúdo': descricao,
            'Registros': count,
            'Colunas': columns,
            'Estado': state,
        })

    unexpected_tables = available_tables.difference(known_tables)
    for table_name in sorted(unexpected_tables):
        qualified_name = f'silver.{table_name}'
        count_query = f'SELECT COUNT(*) AS total FROM silver.{quote_pg_identifier(table_name)}'
        count = int(load_data(count_query).iloc[0]['total'])
        columns = silver_catalog.loc[
            silver_catalog['table_name'] == table_name, 'columns'
        ].iloc[0]
        inventory.append({
            'Tabela': qualified_name,
            'Conteúdo': 'Tabela Silver adicional',
            'Registros': count,
            'Colunas': columns,
            'Estado': 'Disponível',
        })

    inventory_frame = pd.DataFrame(inventory)
    available_count = int((inventory_frame['Estado'] == 'Disponível').sum())
    metric_columns = st.columns(3)
    metric_columns[0].metric('Tabelas disponíveis', format_number(available_count))
    metric_columns[1].metric('Tabelas pendentes', format_number(len(inventory_frame) - available_count))
    metric_columns[2].metric(
        'Registros CRAI 2014-2024',
        format_number(int(load_data(SILVER_COUNT).iloc[0]['total_registros'])),
    )

    st.divider()
    available_inventory = inventory_frame.dropna(subset=['Registros']).sort_values('Registros')
    volume_chart = px.bar(
        available_inventory,
        x='Registros',
        y='Tabela',
        orientation='h',
        title='Registros por tabela Silver',
        text='Registros',
        color_discrete_sequence=[COLORS[0]],
    )
    volume_chart.update_traces(
        texttemplate='%{text:,}',
        textposition='outside',
        hovertemplate='%{y}<br>%{x:,} registros<extra></extra>',
    )
    volume_chart.update_layout(
        height=max(390, 42 * len(available_inventory) + 90),
        margin=dict(l=10, r=65, t=48, b=35),
        xaxis_title='Quantidade de registros',
        yaxis_title=None,
        showlegend=False,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
    )
    st.plotly_chart(volume_chart, use_container_width=True)
    st.dataframe(inventory_frame, hide_index=True, use_container_width=True)

    st.subheader('Indicadores da base CRAI 2014-2024')
    highest = silver_quality.iloc[-1]
    lowest = silver_quality.iloc[0]
    quality_columns = st.columns(2)
    quality_columns[0].metric('Campo mais preenchido', highest['campo'], f"{highest['percentual_preenchido']}%")
    quality_columns[1].metric('Campo com mais ausências', lowest['campo'], f"{format_number(lowest['ausentes'])} ausentes")
    st.dataframe(
        silver_quality.rename(columns={
            'campo': 'Campo',
            'preenchidos': 'Preenchidos',
            'ausentes': 'Ausentes',
            'percentual_preenchido': 'Completude (%)',
        }),
        hide_index=True,
        use_container_width=True,
    )

    st.subheader('Relacionamentos entre fontes')
    st.warning(
        'Não há chaves primárias, estrangeiras ou identificadores compartilhados verificados. '
        'Não é seguro fazer joins entre CRAI, Cadastro Único e Bolsa Família. '
        'Compare essas fontes por indicadores agregados, sem combinar registros individuais.'
    )
    st.info(
        'Os dicionários são metadados, não fatos para somar ou cruzar. '
        'Os dicionários CRAI 2025 t1 e t4 são cópias da mesma origem. '
        'Algumas tabelas apresentam cabeçalhos com separador ponto e vírgula dentro de uma única coluna; '
        'revise a ingestão antes de usar seus campos em análises.'
    )
