import time
from datetime import time as dt_time
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
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

# Função para carregar e filtrar os dados
@st.cache(ttl=600)  # Cache por 10 minutos
def carregar_dados():
    dados = list(collection.find())
    df = pd.DataFrame(dados)
    df['created_at'] = pd.to_datetime(df['created_at'])
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
    # Restante do mapeamento
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
    resumo_meltado = resumo_final.melt(id_vars='template', 
                                       value_vars=['envio', 'resposta', 'tel inválido', 'bloquear', 'fora de contexto', 'saber mais'], 
                                       var_name='tipo_resposta', 
                                       value_name='quantidade')

    fig_barras = px.bar(
        resumo_meltado,
        x='template',
        y='quantidade',
        color='tipo_resposta',
        title='Desempenho por Template e Tipo de Resposta',
        text='quantidade'
    )

    fig_barras.update_layout(
        barmode='group',
        xaxis_title='Template',
        yaxis_title='Quantidade',
        legend_title='Tipo de Resposta',
        height=600,
        width=400
    )

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
# Atualização periódica a cada 10 minutos
# -------------------------------
time.sleep(600)  # Aguarda 10 minutos (600 segundos)
st.experimental_rerun()  # Força a atualização do app
