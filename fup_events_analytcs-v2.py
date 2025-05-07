import time
from datetime import time as dt_time
import pandas as pd
import plotly.express as px
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

# ---------------------------------------
# CORREÇÃO DO DATAFRAME
# ---------------------------------------
df_filtrado = df[
    df['event_name'].str.contains("_", na=False) & 
    ~df['event_name'].str.contains("{", na=False)
]

eventos_validos = [
    'outbound',
    'ativação',
    'outboud',  # possível erro de digitação incluso intencionalmente
    'robo_giovanna_leads_ativos_0opt_in_ativo_10min_v0_Envio',
    'robo_giovanna_leads_ativos_0opt_in_ativo_10min_v0_Resposta'
]

padrao_regex = '|'.join(eventos_validos)

df_filtrado = df_filtrado[
    df_filtrado['event_name'].str.contains(padrao_regex, case=False, na=False)
]


def extrair_template_e_tipo(event_name):
    # Mapeia os eventos específicos de "robo_giovanna_leads_ativos_0opt_in_ativo_10min_v0"
    if 'robo_giovanna_leads_ativos_0opt_in_ativo_10min_v0' in event_name:
        template = 'fup_10min'  # Simplificado para fup_10min para ambos os casos
    else:
        match_template = re.search(
            r'(robo_giovanna_leads_ativos_\d+opt_in_ativo_10min_v\d+_(?:Resposta|Envio)|'
            r'opt_in_ativo(?:_[a-z0-9]+_v\d+)?|'
            r'optin(?:_?[a-z0-9]+)?|'
            r'fup_15_min_v\d+|'
            r'fup[123]_ativo|'
            r'fup2h|'
            r'fup30min|'
            r'optinnegv01|'
            r'neg[123]|'
            r'despedida_ativo|'
            r'perda_sem interação|'
            r'disparo_novo_\d+|'
            r'proposta)',
            event_name,
            re.IGNORECASE
        )
        template = match_template.group(1).lower() if match_template else 'desconhecido'

    # 2. Normalizar o nome do evento para facilitar o match
    lower = event_name.lower()

    # 3. Mapear tipo do evento com base em palavras-chave
    if 'envio' in lower:
        tipo = 'envio'
    elif any(x in lower for x in ['bloquear', 'bloqueio']):
        tipo = 'bloquear'
    elif any(x in lower for x in ['tel.invalido', 'pessoa errada']):
        tipo = 'tel inválido'
    elif any(x in lower for x in ['fora.contexto', 'out.contexto', 'texto']):
        tipo = 'fora de contexto'
    elif any(x in lower for x in ['saber.mais', 'saber mais']):
        tipo = 'saber mais'
    elif any(x in lower for x in ['perda', 'sem interação']):
        tipo = 'sem interação'
    elif any(x in lower for x in ['resposta']):
        tipo = 'resposta'
    else:
        tipo = 'desconhecido'

    # 4. Categoria simplificada: envio, resposta ou desconhecido
    if tipo == 'envio':
        categoria = 'envio'
    elif tipo in ['bloquear', 'tel inválido', 'fora de contexto', 'saber mais', 'resposta']:
        categoria = 'resposta'
    elif tipo in ['sem interação']:
        categoria = 'sem interação'
    else:
        categoria = 'desconhecido'

    return pd.Series([template, tipo, categoria])


df_filtrado[['template', 'tipo', 'categoria']] = df_filtrado['event_name'].apply(extrair_template_e_tipo)
df_filtrado = df_filtrado[df_filtrado['template'] != 'desconhecido'].copy()

# -------------------------------
# FILTRO DE TEMPLATES
# -------------------------------
templates_disponiveis = sorted(df_filtrado['template'].unique())
opcoes_template = ["Todos"] + templates_disponiveis

templates_selecionados = st.sidebar.multiselect(
    "Selecionar templates para análise",
    options=opcoes_template,
    default=["Todos"]
)

# Lógica: se "Todos" está selecionado ou nada selecionado, inclui todos
if "Todos" in templates_selecionados or not templates_selecionados:
    df_filtrado = df_filtrado[df_filtrado['template'].isin(templates_disponiveis)]
else:
    df_filtrado = df_filtrado[df_filtrado['template'].isin(templates_selecionados)]



# -------------------------------
# Gráfico 1: Barras empilhadas + Linha de taxa de resposta
# -------------------------------
distribuicao_resposta = df_filtrado.groupby(['template', 'tipo'])['tipo'].size().unstack(fill_value=0)
distribuicao_resposta_reset = distribuicao_resposta.reset_index()

taxa_resposta = df_filtrado.groupby(['template', 'categoria'])['tipo'].size().unstack(fill_value=0)
taxa_resposta['resposta'] = taxa_resposta.get('resposta', 0)
taxa_resposta['envio'] = taxa_resposta.get('envio', 0)
taxa_resposta['taxa_resposta'] = (taxa_resposta['resposta'] / taxa_resposta['envio']).fillna(0) * 100
taxa_resposta_reset = taxa_resposta[['taxa_resposta']].reset_index()

fig1 = go.Figure()

for tipo in distribuicao_resposta.columns:
    fig1.add_trace(go.Bar(
        x=distribuicao_resposta_reset['template'],
        y=distribuicao_resposta_reset[tipo],
        name=tipo
    ))

fig1.add_trace(go.Scatter(
    x=taxa_resposta_reset['template'],
    y=taxa_resposta_reset['taxa_resposta'],
    mode='lines+markers',
    name='Taxa de Resposta (%)',
    line=dict(color='white', dash='dot'),
    yaxis='y2'
))

fig1.update_layout(
    xaxis=dict(
        showline=False,
        showticklabels=True,
        tickangle=45,
        tickfont=dict(family="Arial", size=12, color="white")
    ),
    yaxis=dict(showgrid=False, zeroline=False, visible=False),
    yaxis2=dict(
        title=" ",
        overlaying="y",
        side="right",
        showgrid=False,
        zeroline=False,
        visible=True,
        tickfont=dict(color="white")
    ),
    barmode='stack',
    height=300,
    legend_title_text="Tipo de Resposta",
    legend=dict(font=dict(color="white")),
    plot_bgcolor='rgba(0,0,0,0)',
    paper_bgcolor='rgba(0,0,0,0)',
    margin=dict(l=0, r=40, t=20, b=40),
    hoverlabel= dict(
        bgcolor="black",
        font=dict(color="white")
    )
)

st.plotly_chart(fig1, use_container_width=True)

# -------------------------------
# Gráfico 2: Taxa de Resposta por Template (Horizontal)
# -------------------------------
taxa_resposta = df_filtrado.groupby(['template', 'categoria'])['tipo'].size().unstack(fill_value=0)
taxa_resposta['resposta'] = taxa_resposta.get('resposta', 0)
taxa_resposta['envio'] = taxa_resposta.get('envio', 0)
taxa_resposta['taxa_resposta'] = (taxa_resposta['resposta'] / taxa_resposta['envio']).fillna(0) * 100
taxa_resposta_reset = taxa_resposta[['taxa_resposta']].reset_index().sort_values('taxa_resposta')

fig2 = go.Figure()

fig2.add_trace(go.Bar(
    x=taxa_resposta_reset['taxa_resposta'],
    y=taxa_resposta_reset['template'],
    orientation='h',
    name='Taxa de Resposta (%)',
    marker=dict(
        color=taxa_resposta_reset['taxa_resposta'],
        colorscale='Blues',
        showscale=True
    ),
))

fig2.update_layout(
    title="Taxa de Resposta por Template",
    xaxis_title="Taxa de Resposta (%)",
    yaxis_title="Template",
    height=300,
    margin=dict(l=40, r=40, t=40, b=40),
)

st.plotly_chart(fig2, use_container_width=True)

# -------------------------------
# Gráfico 3: Taxa de Resposta Semanal por Template
# -------------------------------
df_filtrado['created_at'] = pd.to_datetime(df_filtrado['created_at'])
df_filtrado['semana'] = df_filtrado['created_at'].dt.to_period('W')
taxa_resposta_semanal_template = df_filtrado.groupby(['semana', 'template']).apply(
    lambda x: (x['tipo'] == 'resposta').sum() / (x['tipo'] == 'envio').sum() * 100 if (x['tipo'] == 'envio').sum() > 0 else 0
).reset_index(name='taxa_resposta')
taxa_resposta_semanal_template['semana'] = taxa_resposta_semanal_template['semana'].dt.start_time

fig3 = go.Figure()

for template in taxa_resposta_semanal_template['template'].unique():
    df_template = taxa_resposta_semanal_template[taxa_resposta_semanal_template['template'] == template]
    fig3.add_trace(go.Scatter(
        x=df_template['semana'],
        y=df_template['taxa_resposta'],
        mode='lines+markers',
        name=f'{template} - taxa %',
    ))

fig3.update_layout(
    title="Taxa de Resposta Semanal por Template",
    xaxis_title="Semana",
    yaxis_title="Taxa de Resposta (%)",
    height=300,
    margin=dict(l=40, r=40, t=40, b=40),
    legend_title="Template",
)

st.plotly_chart(fig3, use_container_width=True)

st.write(taxa_resposta_semanal_template)
