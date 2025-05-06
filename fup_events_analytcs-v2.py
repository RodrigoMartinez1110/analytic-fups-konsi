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

df_filtrado = df_filtrado[
    df_filtrado['event_name'].str.contains('outbound|ativação', case=False, na=False)
]


def extrair_template_e_tipo(event_name):
    # 1. Extrair o template com regex
    match_template = re.search(
        r'(opt_in_ativo(?:_30min_v\d+)?|'
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


col1, col2 = st.columns(2)
# Grafico 1
with col1:
    distribuicao_resposta = df_filtrado.groupby(['template', 'tipo'])['tipo'].size().unstack(fill_value=0)
    distribuicao_resposta_reset = distribuicao_resposta.reset_index()
    
    # Agrupar por template e categoria para taxa de resposta
    taxa_resposta = df_filtrado.groupby(['template', 'categoria'])['tipo'].size().unstack(fill_value=0)
    taxa_resposta['resposta'] = taxa_resposta.get('resposta', 0)
    taxa_resposta['envio'] = taxa_resposta.get('envio', 0)
    taxa_resposta['taxa_resposta'] = (taxa_resposta['resposta'] / taxa_resposta['envio']).fillna(0) * 100
    taxa_resposta_reset = taxa_resposta[['taxa_resposta']].reset_index()
    
    # Criar gráfico
    fig = go.Figure()

    for tipo in distribuicao_resposta.columns:
        fig.add_trace(go.Bar(
            x=distribuicao_resposta_reset['template'],
            y=distribuicao_resposta[tipo],
            name=tipo
        ))
    
        # Linha da taxa de resposta
        fig.add_trace(go.Scatter(
            x=taxa_resposta_reset['template'],
            y=taxa_resposta_reset['taxa_resposta'],
            mode='lines+markers',
            name='Taxa de Resposta (%)',
            line=dict(color='white', dash='dot'),
            yaxis='y2'
        ))
        
        # Layout geral
        fig.update_layout(
            xaxis=dict(
                showline=False,
                showticklabels=True,
                tickangle=45
            ),
            yaxis=dict(
                showgrid=False,
                zeroline=False,
                visible=False  # Oculta o eixo Y principal
            ),
            yaxis2=dict(
                title=" ",
                overlaying="y",
                side="right",
                showgrid=False,
                zeroline=False,
                visible=True
            ),
            barmode='stack',
            height=400,
            legend_title_text="Tipo de Resposta",
            legend=dict(font=dict(color="white")),
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            margin=dict(l=0, r=40, t=0, b=40),
        )
        
        # Exibindo o gráfico
        fig.update_xaxes(tickangle=45)
        st.plotly_chart(fig)


# Coluna 2: Gráfico de Taxa de Resposta
with col2:
    # Agrupar por template e categoria, contando os tipos para calcular a taxa de resposta
    taxa_resposta = df_filtrado.groupby(['template', 'categoria'])['tipo'].size().unstack(fill_value=0)

    # Calcular a taxa de resposta
    taxa_resposta['resposta'] = taxa_resposta.get('resposta', 0)
    taxa_resposta['envio'] = taxa_resposta.get('envio', 0)

    # Calcular a taxa de resposta (evitar divisão por 0)
    taxa_resposta['taxa_resposta'] = (taxa_resposta['resposta'] / taxa_resposta['envio']).fillna(0) * 100

    # Resetando o índice para facilitar o uso no gráfico
    taxa_resposta_reset = taxa_resposta[['taxa_resposta']].reset_index().sort_values('taxa_resposta')

    # Criando o gráfico de barras horizontais
    fig = go.Figure()

    # Adicionando as barras horizontais com gradiente de cores
    fig.add_trace(go.Bar(
        x=taxa_resposta_reset['taxa_resposta'],  # Eixo x: taxa de resposta
        y=taxa_resposta_reset['template'],       # Eixo y: templates
        orientation='h',                         # Barras horizontais
        name='Taxa de Resposta (%)',
        marker=dict(
            color=taxa_resposta_reset['taxa_resposta'],  # Cor baseada na taxa de resposta
            colorscale='Blues',  # Escolha um gradiente de cores como 'Blues', 'Viridis', etc.
            showscale=True       # Exibe a barra de cores
        ),
    ))

    # Atualizando o layout do gráfico
    fig.update_layout(
        title="Taxa de Resposta por Template",
        xaxis_title="Taxa de Resposta (%)",
        yaxis_title="Template",
        height=600,
        margin=dict(l=40, r=40, t=60, b=40),
    )

    # Exibindo o gráfico
    st.plotly_chart(fig)


# -------------------------------
# Gráfico Temporal (Taxa de Resposta por Semana)
# -------------------------------
# Agrupando por semana para calcular a taxa de resposta
# Garantir que a coluna 'created_at' está no formato datetime
df_filtrado['created_at'] = pd.to_datetime(df_filtrado['created_at'])

# Criar a coluna 'semana' usando a coluna 'created_at'
df_filtrado['semana'] = df_filtrado['created_at'].dt.to_period('W')

# Agrupar por semana e template, e calcular a taxa de resposta
taxa_resposta_semanal_template = df_filtrado.groupby(['semana', 'template']).apply(
    lambda x: (x['tipo'] == 'resposta').sum() / (x['tipo'] == 'envio').sum() * 100 if (x['tipo'] == 'envio').sum() > 0 else 0
).reset_index(name='taxa_resposta')

# Convertendo 'semana' de Period para Datetime para o gráfico
taxa_resposta_semanal_template['semana'] = taxa_resposta_semanal_template['semana'].dt.start_time

# Criando o gráfico de linha para cada template
fig = go.Figure()

# Adicionando uma linha para cada template
for template in taxa_resposta_semanal_template['template'].unique():
    df_template = taxa_resposta_semanal_template[taxa_resposta_semanal_template['template'] == template]
    fig.add_trace(go.Scatter(
        x=df_template['semana'],
        y=df_template['taxa_resposta'],
        mode='lines+markers',
        name=f'{template} - taxa %',
    ))

# Atualizando o layout do gráfico
fig.update_layout(
    title="Taxa de Resposta Semanal por Template",
    xaxis_title="Semana",
    yaxis_title="Taxa de Resposta (%)",
    height=600,
    margin=dict(l=40, r=40, t=60, b=40),
    legend_title="Template",
)

# Exibindo o gráfico
st.plotly_chart(fig)
