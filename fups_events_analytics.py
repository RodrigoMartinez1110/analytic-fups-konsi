import time
from datetime import time as dt_time
import pandas as pd
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import streamlit as st
from streamlit_autorefresh import st_autorefresh
from pymongo import MongoClient
import re

# -------------------------------
# Conexão com o MongoDB
# -------------------------------
uri = st.secrets["mongodb"]["uri"]
client = MongoClient(uri)
db = client["growth"]
collection = db["events"]

st.set_page_config(
    page_title="Meu Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Atualização periódica a cada 10 minutos
_ = st_autorefresh(interval=600_000, limit=None, key="auto_refresh")

# Função para carregar e filtrar os dados
@st.cache_resource
def get_client():
    return MongoClient(uri)

@st.cache_data(ttl=600)
def carregar_dados():
    client = get_client()
    dados = list(client["growth"]["events"].find())
    df = pd.DataFrame(dados)
    df["created_at"] = pd.to_datetime(df["created_at"])
    return df

# Carrega os dados
df = carregar_dados()

# -------------------------------
# Filtros de data e horário
# -------------------------------
data_min = df['created_at'].min().date()
data_max = df['created_at'].max().date()
data_inicio = st.sidebar.date_input("Data inicial", value=data_min, min_value=data_min, max_value=data_max)
data_fim = st.sidebar.date_input("Data final", value=data_max, min_value=data_min, max_value=data_max)
df_filtrado = df[(df['created_at'].dt.date >= data_inicio) & (df['created_at'].dt.date <= data_fim)]

hora_inicio = st.sidebar.time_input("Hora inicial", value=dt_time(0, 1))
hora_fim = st.sidebar.time_input("Hora final", value=dt_time(23, 59))
df_filtrado = df_filtrado[(df_filtrado['created_at'].dt.time >= hora_inicio) & (df_filtrado['created_at'].dt.time <= hora_fim)]

# -------------------------------
# Remove eventos irrelevantes
# -------------------------------
df_filtrado = df_filtrado.loc[
    ~df_filtrado['event_name'].str.contains(
        r"\{|\[OUTBOUND\] FLUXO LEAD|excedeu tentativas.*?atendimento humano|ativação",
        na=False, case=False
    )
]

# -------------------------------
# Mapeamento e extração de informações
# -------------------------------
mapeamento_invertido = {
    "opt-in ativo saber mais": "robo_giovanna_leads_ativos_0_opt_in_ativo_Resposta_Saber mais",
    "opt-in pessoa errada": "robo_giovanna_leads_ativos_0_opt_in_ativo_Resposta_Pessoa errada",
    "opt-in bloquear mensagens": "robo_giovanna_leads_ativos_0_opt_in_ativo_Resposta_Bloqueio",
    "OPT_IN Resposta": "robo_giovanna_leads_ativos_0_opt_in_ativo_Resposta_Texto",
    "opt-in ativo fup2": "robo_giovanna_leads_ativos_0fup2_ativo_Envio",
    "FUP 2 resposta": "robo_giovanna_leads_ativos_0fup2_ativo_Resposta",
    "opt-in ativo despedida": "robo_giovanna_leads_ativos_0despedida_ativo_Envio",
    "Despedida resposta": "robo_giovanna_leads_ativos_0despedida_ativo_Resposta",
    "opt-in ativo fup3": "robo_giovanna_leads_ativos_0fup3_ativo_Envio",
    "FUP 3 resposta": "robo_giovanna_leads_ativos_0fup3_ativo_Resposta",
    "opt-in ativo fup 30min": "robo_giovanna_leads_ativos_0opt_in_ativo_30min_v0_Envio",
    "FUP 30min resposta": "robo_giovanna_leads_ativos_0opt_in_ativo_30min_v0_Resposta",
    "opt-in ativo fup1": "robo_giovanna_leads_ativos_0fup1_ativo_Envio",
    "FUP 1 resposta": "robo_giovanna_leads_ativos_0fup1_ativo_Resposta",
}

df_filtrado['event_name'] = df_filtrado['event_name'].map(mapeamento_invertido).fillna(df_filtrado['event_name'])

def extrair_template_e_tipo(event_name):
    match_template = re.search(r'(opt_in_ativo(?:_30min_v\d+)?|fup_15_min_v\d+|fup[123]_ativo)', event_name, re.IGNORECASE)
    template = match_template.group(1).lower() if match_template else 'desconhecido'
    lower = event_name.lower()
    if 'envio' in lower: tipo = 'envio'
    elif 'bloquear' in lower or 'bloqueio' in lower: tipo = 'bloquear'
    elif 'saber.mais' in lower or 'saber mais' in lower: tipo = 'saber mais'
    elif 'pessoa errada' in lower: tipo = 'pessoa errada'
    elif 'texto' in lower: tipo = 'texto'
    elif 'tel.invalido' in lower or 'tel_invalido' in lower: tipo = 'tel inválido'
    elif 'out.contexto' in lower or 'fora.contexto' in lower: tipo = 'fora de contexto'
    elif 'resposta' in lower: tipo = 'resposta'
    else: tipo = 'desconhecido'
    return pd.Series([template, tipo])

df_filtrado[['template', 'tipo_evento']] = df_filtrado['event_name'].apply(extrair_template_e_tipo)
df_filtrado = df_filtrado[df_filtrado['template'] != 'desconhecido']

# -------------------------------
# Resumo por template e tipo
# -------------------------------
tipos_resposta = ['resposta', 'bloquear', 'tel inválido', 'fora de contexto', 'saber mais']
resumo = df_filtrado.groupby(['template', 'tipo_evento']).size().unstack(fill_value=0)

for tipo in tipos_resposta:
    if tipo not in resumo.columns:
        resumo[tipo] = 0

resumo['resposta'] = resumo[tipos_resposta].sum(axis=1)
resumo['envio'] = resumo.get('envio', 0)
resumo['taxa_resposta'] = (resumo['resposta'] / resumo['envio']).fillna(0) * 100
resumo_final = resumo[['envio', 'resposta', 'tel inválido', 'bloquear', 'fora de contexto', 'saber mais', 'taxa_resposta']].reset_index()

# -------------------------------
# Exibição dos Gráficos
# -------------------------------
col1, col2 = st.columns(2)

# Coluna 1: Gráfico de barras por tipo de resposta
with col1:
    # Cria figura com eixo y primário e secundário
    fig_barras = make_subplots(specs=[[{"secondary_y": True}]])

    # Lista das categorias de contagem
    categorias = [
        'envio',
        'resposta',
        'tel inválido',
        'bloquear',
        'fora de contexto',
        'saber mais'
    ]

    # Adiciona cada barra ao eixo principal (y esquerdo)
    for cat in categorias:
        fig_barras.add_trace(
            go.Bar(
                x=resumo_final['template'],
                y=resumo_final[cat],
                name=cat.capitalize(),
                text=resumo_final[cat],
                textposition="auto"
            ),
            secondary_y=False
        )

    # Adiciona a linha de taxa de resposta ao eixo secundário (y direito)
    fig_barras.add_trace(
        go.Scatter(
            x=resumo_final['template'],
            y=resumo_final['taxa_resposta'],
            name='Taxa de Resposta (%)',
            mode='lines+markers',
            marker=dict(size=8),
            yaxis='y2'
        ),
        secondary_y=True
    )

    # Layout geral
    fig_barras.update_layout(
        title='Desempenho por Template e Tipo de Resposta (com Taxa)',
        barmode='group',
        xaxis_title='Template',
        legend_title='Métricas',
        height=600,
    )

    # Títulos dos eixos e remoção das grades
    fig_barras.update_xaxes(title_text='Template', showgrid=False)
    fig_barras.update_yaxes(title_text='Quantidade de Eventos', secondary_y=False, showgrid=False)
    fig_barras.update_yaxes(title_text='Taxa de Resposta (%)', secondary_y=True, showgrid=False)

    st.plotly_chart(fig_barras, use_container_width=True)

# Coluna 2: Gráfico de Taxa de Resposta
with col2:
    resumo_final['taxa_resposta'] = ((resumo_final['resposta'] / resumo_final['envio']).fillna(0) * 100).round(2)
    resumo_final = resumo_final.sort_values('taxa_resposta', ascending=True)

    fig_taxa_resposta = px.bar(
        resumo_final,
        x='taxa_resposta',
        y='template',
        orientation='h',
        title='Taxa de Resposta por Template (%)',
        text='taxa_resposta'
    )

    fig_taxa_resposta.update_layout(
        xaxis_title='Taxa de Resposta (%)',
        yaxis_title='Template',
        height=600,
        width=450
    )

    fig_taxa_resposta.update_traces(
        textfont=dict(size=16, color='white'),
        textposition='outside'
    )

    st.plotly_chart(fig_taxa_resposta, use_container_width=True)

# -------------------------------
# Gráfico Temporal (Taxa de Resposta por Semana)
# -------------------------------
# Agrupando por semana para calcular a taxa de resposta
df_filtrado['semana'] = df_filtrado['created_at'].dt.to_period('W')

# Calculando a taxa de resposta semanal por template
df_semana = df_filtrado.groupby(['semana', 'template']).agg(
    resposta=('tipo_evento', lambda x: (x == 'resposta').sum()),
    envio=('tipo_evento', lambda x: (x == 'envio').sum())
).reset_index()

# Calculando a taxa de resposta semanal
df_semana['taxa_resposta_semanal'] = (df_semana['resposta'] / df_semana['envio']) * 100
df_semana['semana'] = df_semana['semana'].dt.strftime('%Y-%m-%d')

# Gráfico de linha temporal da taxa de resposta
fig_temporal = px.line(
    df_semana,
    x='semana',
    y='taxa_resposta_semanal',
    color='template',
    title='Taxa de Resposta Geral por Semana',
    labels={'taxa_resposta_semanal': 'Taxa de Resposta (%)', 'semana': 'Semana'},
    markers=True
)

fig_temporal.update_layout(
    xaxis_title='Semana',
    yaxis_title='Taxa de Resposta (%)',
    height=600,
    width=800
)

st.plotly_chart(fig_temporal, use_container_width=True)


