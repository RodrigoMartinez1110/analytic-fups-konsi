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
df_filtrado = df_filtrado[
    df_filtrado['event_name'].str.contains("_", na=False) & 
    ~df_filtrado['event_name'].str.contains("{", na=False)
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
# AJUSTE DE NOME DOS EVENTOS
# -------------------------------
NOMES_RESUMIDOS = {
    "Outbound_qualificado_optinnegv01_envio_v1": "Qualificado msg 1",
    "Outbound_qualificado_optinnegv01_resposta_v1": "Qualificado msg 1",
    "Outbound_qualificado_fup30min_envio_v1": "Qualificado fup30min",
    "Outbound_qualificado_fup30min_resposta_v1": "Qualificado fup30min",
    "Outbound_qualificado_fup2h_envio_v1": "Qualificado fup2h",
    "Outbound_qualificado_fup2h_resposta_v1": "Qualificado fup2h",
    "Outbound_qualificado_neg1_envio_v1": "Qualificado neg1",
    "Outbound_qualificado_neg1_resposta_v1": "Qualificado neg1",
    "Outbound_qualificado_neg2_envio_v1": "Qualificado neg2",
    "Outbound_qualificado_neg2_resposta_v1": "Qualificado neg2",
    "Outbound_qualificado_neg3_envio_v1": "Qualificado neg3",
    "Outbound_qualificado_neg3_resposta_v1": "Qualificado neg3",
    "Outbound_qualificado_neg_despedida_envio_v1": "Qualificado despedida",
    "Outbound_qualificado_neg_despedida_resposta_v1": "Qualificado despedida",
    "Outbound_qualificado_perda_Perda_v": "Qualificado perda"
}

df_filtrado['nome_exibicao'] = df_filtrado['event_name'].map(NOMES_RESUMIDOS).fillna(df_filtrado['template'])

# -------------------------------
# FILTRO DE TEMPLATES
# -------------------------------
templates_disponiveis = sorted(df_filtrado['nome_exibicao'].unique())
opcoes_template = ["Todos"] + templates_disponiveis

templates_selecionados = st.sidebar.multiselect(
    "Selecionar templates para análise",
    options=["Todos"] + templates_disponiveis,
    default=["Todos"]
)

if "Todos" in templates_selecionados or not templates_selecionados:
    df_filtrado = df_filtrado[df_filtrado['nome_exibicao'].isin(templates_disponiveis)]
else:
    df_filtrado = df_filtrado[df_filtrado['nome_exibicao'].isin(templates_selecionados)]






# -------------------------------
# Gráfico 1: Barras empilhadas + Linha de taxa de resposta
# -------------------------------
distribuicao_resposta = df_filtrado.groupby(['nome_exibicao', 'tipo'])['tipo'].size().unstack(fill_value=0)
distribuicao_resposta_reset = distribuicao_resposta.reset_index()

taxa_resposta = df_filtrado.groupby(['nome_exibicao', 'categoria'])['tipo'].size().unstack(fill_value=0)
taxa_resposta['resposta'] = taxa_resposta.get('resposta', 0)
taxa_resposta['envio'] = taxa_resposta.get('envio', 0)
taxa_resposta['taxa_resposta'] = (taxa_resposta['resposta'] / taxa_resposta['envio']).fillna(0) * 100
taxa_resposta_reset = taxa_resposta[['taxa_resposta']].reset_index()

fig1 = go.Figure()

for tipo in distribuicao_resposta.columns:
    fig1.add_trace(go.Bar(
        x=distribuicao_resposta_reset['nome_exibicao'],
        y=distribuicao_resposta_reset[tipo],
        name=tipo
    ))

fig1.add_trace(go.Scatter(
    x=taxa_resposta_reset['nome_exibicao'],
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
# Gráfico 3: Taxa de Resposta por Template (Horizontal)
# -------------------------------

# Criar uma coluna de data com o dia sem a hora
df_filtrado['data'] = df_filtrado['created_at'].dt.date

# Criar uma coluna que marca a semana (usando .dt.to_period('W'))
df_filtrado['semana'] = df_filtrado['created_at'].dt.to_period('W').dt.start_time

# Calcular a taxa de resposta por semana
taxa_resposta_semanal = df_filtrado.groupby(['semana', 'nome_exibicao']).apply(
    lambda x: (x['categoria'] == 'resposta').sum() / (x['categoria'] == 'envio').sum() * 100 if (x['categoria'] == 'envio').sum() > 0 else 0
).reset_index(name='taxa_resposta')

# Filtrar para taxas de resposta <= 100%
taxa_resposta_semanal = taxa_resposta_semanal.loc[taxa_resposta_semanal['taxa_resposta'] <= 100].sort_values(by='semana')

# Criar o gráfico
fig3_semanal = go.Figure()

# Adicionar traços para cada template
for nome in taxa_resposta_semanal['nome_exibicao'].unique():
    df_temp = taxa_resposta_semanal[taxa_resposta_semanal['nome_exibicao'] == nome]
    fig3_semanal.add_trace(go.Scatter(
        x=df_temp['semana'],
        y=df_temp['taxa_resposta'],
        mode='lines+markers',
        name=nome
    ))

# Atualizar layout
fig3_semanal.update_layout(
    title="Taxa de Resposta Semanal por Template",
    xaxis_title="Semana",
    yaxis_title="Taxa de Resposta (%)",
    yaxis=dict(
        range=[0, 100],
        dtick=5,
        ticks="outside",
        tickformat=".0f"
    ),
    height=500,
    showlegend=True  # Mostrar legenda
)

# Exibir o gráfico no Streamlit
st.plotly_chart(fig3_semanal, use_container_width=True)


st.write(df_filtrado['event_name'])
st.write(df_filtrado['nome_exibicao'])