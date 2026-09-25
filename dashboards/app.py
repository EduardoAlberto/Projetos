from __future__ import annotations

import streamlit as st
import plotly.express as px

from src.db import get_pool, read_query
from src.queries import (
    AGES,
    AGES_2025,
    BASE_2025_COUNT,
    BASE_2025_SAMPLE,
    COUNTRIES,
    COUNTRIES_2025,
    DEMANDS,
    DEMANDS_2025,
    GENDER,
    GENDER_2025,
    HOUSING,
    HOUSING_2025,
    MIGRATION,
    MIGRATION_2025,
    SUMMARY,
    SUMMARY_2025,
)

st.set_page_config(
    page_title='CRAI | Dashboards',
    page_icon='📊',
    layout='wide',
    initial_sidebar_state='expanded',
)

COLORS = ['#087f73', '#e66b52', '#d99b35', '#24445c', '#62a89f', '#c54f39']


def format_number(value: int) -> str:
    return f'{int(value):,}'.replace(',', '.')


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

    st.divider()
    section = st.radio(
        'Navegação',
        ['Panorama dos atendimentos', 'Base CRAI 2025'],
        index=0,
        label_visibility='collapsed',
    )

if section == 'Panorama dos atendimentos':
    st.markdown('<p class="eyebrow">BASE CRAI 2014 A 2024</p>', unsafe_allow_html=True)
    st.title('Panorama dos atendimentos')
    st.caption('Dashboard conectado ao PostgreSQL | fonte: bronze.bancocrai2014a2024_sistematizacao_geoinfo_atualizada')

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

    st.info('Os países foram agrupados ignorando maiúsculas, minúsculas e espaços externos. A base ainda possui variações de acentuação e nomenclatura que devem ser tratadas na camada silver antes de análises oficiais.')
else:
    st.markdown('<p class="eyebrow">BASE CRAI A PARTIR DE 2025</p>', unsafe_allow_html=True)
    st.title('Panorama dos atendimentos | 2025')
    st.caption('Dashboard conectado ao PostgreSQL | fonte: bronze.base_de_dados_crai_a_partir_de_2025')

    try:
        summary_2025 = load_data(SUMMARY_2025).iloc[0]
        demands_2025 = load_data(DEMANDS_2025)
        countries_2025 = load_data(COUNTRIES_2025)
        ages_2025 = load_data(AGES_2025)
        housing_2025 = load_data(HOUSING_2025)
        migration_2025 = load_data(MIGRATION_2025)
        gender_2025 = load_data(GENDER_2025)
    except Exception:
        st.error('Não foi possível consultar a tabela bronze.base_de_dados_crai_a_partir_de_2025. Verifique a conexão e a estrutura da tabela no PostgreSQL.')
        st.stop()

    metric_columns = st.columns(4)
    metric_columns[0].metric('Registros', format_number(summary_2025['total_registros']))
    metric_columns[1].metric('Regularização migratória', format_number(summary_2025['regularizacao_migratoria']))
    metric_columns[2].metric('País não informado', format_number(summary_2025['pais_nao_informado']))
    metric_columns[3].metric('Escolaridade não informada', format_number(summary_2025['escolaridade_nao_informada']))

    st.divider()
    first_row = st.columns(2)
    with first_row[0]:
        st.plotly_chart(horizontal_chart(demands_2025, 'Principais demandas', COLORS[0]), use_container_width=True)
    with first_row[1]:
        st.plotly_chart(horizontal_chart(countries_2025, 'Países de origem', COLORS[1]), use_container_width=True)

    second_row = st.columns(2)
    with second_row[0]:
        st.plotly_chart(horizontal_chart(ages_2025, 'Faixa etária', COLORS[3]), use_container_width=True)
    with second_row[1]:
        st.plotly_chart(horizontal_chart(housing_2025, 'Condição de moradia', COLORS[2]), use_container_width=True)

    third_row = st.columns(2)
    with third_row[0]:
        st.plotly_chart(horizontal_chart(migration_2025, 'Situação migratória', COLORS[4]), use_container_width=True)
    with third_row[1]:
        st.plotly_chart(horizontal_chart(gender_2025, 'Distribuição por sexo', COLORS[5]), use_container_width=True)

    st.info('Dados da tabela bronze.base_de_dados_crai_a_partir_de_2025. Os países foram agrupados ignorando maiúsculas, minúsculas e espaços externos.')
