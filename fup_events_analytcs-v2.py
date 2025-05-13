import time
from datetime import time as dt_time
import pandas as pd
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
# Correção do DataFrame
# ---------------------------------------
df_filtrado = df_filtrado[
    df_filtrado['event_name'].str.contains("_", na=False) &
    ~df_filtrado['event_name'].str.contains("{", na=False)
]

eventos_validos = [
    'outbound', 'ativação', 'outboud', 'cad'
]

padrao_regex = '|'.join(eventos_validos)

df_filtrado = df_filtrado[
    df_filtrado['event_name'].str.contains(padrao_regex, case=False, na=False)
]

def extrair_template_e_tipo(event_name):
    event_name = event_name.lower()

    # Regex ajustada e mais flexível
    match_template = re.search(
        r'(fup_15_min_v[1-6]_outboud_tx_resp_(?:envio_|resposta_[a-z.]+|resposta_)|'
        r'outbound_giovanna[_a-z0-9]*opt_in_ativo[_a-z0-9]*|'
        r'opt_in_1st_cad_v[0-9]_(?:envio|resposta)|'
        r'outbound_qualificado_(optinnegv01|fup30min|fup2h|neg1|neg2|neg3|neg_despedida|perda)(?:_[a-z]+)?_?(?:envio|resposta)?(?:_siape)?_?v\d*|'
        r'optin(?:_?[a-z0-9]+)?)',
        event_name
    )

    template = match_template.group(1) if match_template else 'desconhecido'

    # Tipo
    if 'envio' in event_name:
        tipo = 'envio'
    elif 'bloquear' in event_name or 'bloqueio' in event_name:
        tipo = 'bloquear'
    elif 'tel.invalido' in event_name or 'pessoa errada' in event_name:
        tipo = 'tel inválido'
    elif 'out.contexto' in event_name or 'fora.contexto' in event_name or 'texto' in event_name:
        tipo = 'fora de contexto'
    elif 'saber.mais' in event_name or 'saber mais' in event_name:
        tipo = 'saber mais'
    elif 'sem interação' in event_name or 'perda' in event_name:
        tipo = 'sem interação'
    elif 'resposta' in event_name:
        tipo = 'resposta'
    else:
        tipo = 'desconhecido'

    # Categoria
    if tipo == 'envio':
        categoria = 'envio'
    elif tipo in ['bloquear', 'tel inválido', 'fora de contexto', 'saber mais', 'resposta']:
        categoria = 'resposta'
    elif tipo == 'sem interação':
        categoria = 'sem interação'
    else:
        categoria = 'desconhecido'

    return pd.Series([template, tipo, categoria])

df_filtrado[['template', 'tipo', 'categoria']] = df_filtrado['event_name'].apply(extrair_template_e_tipo)
df_filtrado = df_filtrado[df_filtrado['template'] != 'desconhecido']



NOMES_RESUMIDOS = {
    # Qualificado
    "outbound_qualificado_optinnegv01_envio_v1": "Qualificado msg 1",
    "outbound_qualificado_optinnegv01_resposta_v1": "Qualificado msg 1",
    "outbound_qualificado_optinnegv01_envio_siape_v1": "Qualificado msg 1 siape",
    "outbound_qualificado_optinnegv01_resposta_siape_v1": "Qualificado msg 1 siape",
    "outbound_qualificado_fup30min_envio_v1": "Qualificado fup30min",
    "outbound_qualificado_fup30min_resposta_v1": "Qualificado fup30min",
    "outbound_qualificado_fup2h_envio_v1": "Qualificado fup2h",
    "outbound_qualificado_fup2h_resposta_v1": "Qualificado fup2h",
    "outbound_qualificado_neg1_envio_v1": "Qualificado neg1",
    "outbound_qualificado_neg1_resposta_v1": "Qualificado neg1",
    "outbound_qualificado_neg2_envio_v1": "Qualificado neg2",
    "outbound_qualificado_neg2_resposta_v1": "Qualificado neg2",
    "outbound_qualificado_neg3_envio_v1": "Qualificado neg3",
    "outbound_qualificado_neg3_resposta_v1": "Qualificado neg3",
    "outbound_qualificado_neg_despedida_envio_v1": "Qualificado despedida",
    "outbound_qualificado_neg_despedida_resposta_v1": "Qualificado despedida",
    "outbound_qualificado_perda_perda_v": "Qualificado perda",

    # FUP 15 min
    "fup_15_min_v1_outboud_tx_resp_envio_": "FUP 15min v1",
    "fup_15_min_v1_outboud_tx_resp_resposta_": "FUP 15min v1",
    "fup_15_min_v1_outboud_tx_resp_resposta_tel.invalido": "FUP 15min v1",
    "fup_15_min_v1_outboud_tx_resp_resposta_bloquear": "FUP 15min v1",
    "fup_15_min_v1_outboud_tx_resp_resposta_out.contexto": "FUP 15min v1",
    "fup_15_min_v1_outboud_tx_resp_resposta_saber.mais": "FUP 15min v1",
    "fup_15_min_v2_outboud_tx_resp_envio_": "FUP 15min v2",
    "fup_15_min_v2_outboud_tx_resp_resposta_tel.invalido": "FUP 15min v2",
    "fup_15_min_v2_outboud_tx_resp_resposta_bloquear": "FUP 15min v2",
    "fup_15_min_v2_outboud_tx_resp_resposta_out.contexto": "FUP 15min v2",
    "fup_15_min_v2_outboud_tx_resp_resposta_saber.mais": "FUP 15min v2",
    "fup_15_min_v3_outboud_tx_resp_envio_": "FUP 15min v3",
    "fup_15_min_v3_outboud_tx_resp_resposta_": "FUP 15min v3",
    "fup_15_min_v4_outboud_tx_resp_envio_": "FUP 15min v4",
    "fup_15_min_v4_outboud_tx_resp_resposta_tel.invalido": "FUP 15min v4",
    "fup_15_min_v4_outboud_tx_resp_resposta_bloquear": "FUP 15min v4",
    "fup_15_min_v4_outboud_tx_resp_resposta_out.contexto": "FUP 15min v4",
    "fup_15_min_v4_outboud_tx_resp_resposta_saber.mais": "FUP 15min v4",
    "fup_15_min_v5_outboud_tx_resp_envio_": "FUP 15min v5",
    "fup_15_min_v5_outboud_tx_resp_resposta_": "FUP 15min v5",
    "fup_15_min_v6_outboud_tx_resp_envio_": "FUP 15min v6",
    "fup_15_min_v6_outboud_tx_resp_resposta_": "FUP 15min v6",

    # Giovanna
    "outbound_giovanna__opt_in_ativo__envio_v1": "Giovanna optin",
    "outbound_giovanna__opt_in_ativo__resposta_saber mais_v1": "Giovanna optin",
    "outbound_giovanna__opt_in_ativo__resposta_pessoa errada_v1": "Giovanna optin",
    "outbound_giovanna__opt_in_ativo__resposta_bloqueio_v1": "Giovanna optin",
    "outbound_giovanna__opt_in_ativo__resposta_texto_v1": "Giovanna optin",
    "outbound_giovanna_opt_in_ativo_30min_v0__envio_v1": "Giovanna optin 30min",
    "outbound_giovanna_opt_in_ativo_30min_v0__resposta_v1": "Giovanna optin 30min",
    "outbound_giovanna_opt_in_ativo_10min_v0__envio_v1": "Giovanna optin 10min",
    "outbound_giovanna_opt_in_ativo_10min_v0__resposta_v1": "Giovanna optin 10min",

    # Optin 1st cad
    "opt_in_1st_cad_v0_envio": "Optin 1st cad v0",
    "opt_in_1st_cad_v0_resposta": "Optin 1st cad v0",
    "opt_in_1st_cad_v1_envio": "Optin 1st cad v1",
    "opt_in_1st_cad_v1_resposta": "Optin 1st cad v1",
    "opt_in_1st_cad_v2_envio": "Optin 1st cad v2",
    "opt_in_1st_cad_v2_resposta": "Optin 1st cad v2",
    "opt_in_1st_cad_v3_envio": "Optin 1st cad v3",
    "opt_in_1st_cad_v3_resposta": "Optin 1st cad v3"
}

df_filtrado['nome_exibicao'] = df_filtrado['event_name'].map(NOMES_RESUMIDOS).fillna(df_filtrado['template'])



# -------------------------------
# Filtro de templates
# -------------------------------
templates_disponiveis = sorted(df_filtrado['nome_exibicao'].unique())
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
# Gráfico 1: Barras empilhadas + linha
# -------------------------------
# Agrupamento por tipo
distribuicao_resposta = df_filtrado.groupby(['nome_exibicao', 'tipo'])['tipo'] \
    .size().unstack(fill_value=0).reset_index()

# Agrupamento para taxa de resposta
taxa_resposta = df_filtrado.groupby(['nome_exibicao', 'categoria'])['tipo'] \
    .size().unstack(fill_value=0).reset_index()

# Garantir colunas
taxa_resposta['resposta'] = taxa_resposta.get('resposta', 0)
taxa_resposta['envio'] = taxa_resposta.get('envio', 0)
taxa_resposta['taxa_resposta'] = (taxa_resposta['resposta'] / taxa_resposta['envio']) \
    .replace([float('inf'), float('nan')], 0) * 100

# Seleciona somente taxa + nome
taxa_resposta = taxa_resposta[['nome_exibicao', 'taxa_resposta']]

# Ordena pela soma dos eventos (opcional)
distribuicao_resposta['total'] = distribuicao_resposta.drop(columns='nome_exibicao').sum(axis=1)
distribuicao_resposta = distribuicao_resposta.sort_values(by='total', ascending=False).drop(columns='total')

# Alinha taxa_resposta com a mesma ordem de nome_exibicao
taxa_resposta = taxa_resposta.set_index('nome_exibicao').loc[distribuicao_resposta['nome_exibicao']].reset_index()

# Cores suaves
cores_discretas = px.colors.qualitative.Set2

# Figura
fig1 = go.Figure()

# Adiciona as barras empilhadas por tipo
for tipo in distribuicao_resposta.columns[1:]:
    fig1.add_trace(go.Bar(
        x=distribuicao_resposta['nome_exibicao'],
        y=distribuicao_resposta[tipo],
        name=tipo,
        hovertemplate=(
            "<b>Template:</b> %{x}<br>" +
            "<b>Tipo:</b> " + tipo + "<br>" +
            "<b>Quantidade:</b> %{y}<extra></extra>"
        )
    ))

# Adiciona a linha de taxa de resposta
fig1.add_trace(go.Scatter(
    x=taxa_resposta['nome_exibicao'],
    y=taxa_resposta['taxa_resposta'],
    mode='lines+markers',
    name='Taxa de Resposta (%)',
    line=dict(color='white', dash='dot'),
    yaxis='y2',
    hovertemplate=(
        "<b>Template:</b> %{x}<br>" +
        "<b>Taxa de Resposta:</b> %{y:.2f}%<extra></extra>"
    )
))

# Layout final
fig1.update_layout(
    title='',  # Título removido
    xaxis=dict(
        title='Template',
        tickangle=45,
        tickfont=dict(size=10),
    ),
    yaxis=dict(
        title='Quantidade de Eventos'
    ),
    yaxis2=dict(
        title='Taxa de Resposta (%)',
        overlaying="y",
        side="right",
        showgrid=False
    ),
    barmode='stack',
    height=550,
    width=1100,
    legend=dict(
        orientation='h',
        yanchor='top',
        y=1.1,     # Legenda abaixo do gráfico
        xanchor='center',
        x=0.5,
        font=dict(size=10)
    ),
    margin=dict(t=30, l=60, r=60, b=130)
)

st.plotly_chart(fig1, use_container_width=True)








# -------------------------------
# Gráfico 2: Taxa por template
# -------------------------------
# Recria o DataFrame com taxa
taxa_template = df_filtrado.groupby(['nome_exibicao', 'categoria'])['tipo'].size().unstack(fill_value=0)
taxa_template['resposta'] = taxa_template.get('resposta', 0)
taxa_template['envio'] = taxa_template.get('envio', 0)
taxa_template['taxa_resposta'] = (taxa_template['resposta'] / taxa_template['envio']).fillna(0) * 100

# Ordena por taxa de resposta
taxa_template = taxa_template[['taxa_resposta']].reset_index().sort_values('taxa_resposta')

# Calcula altura baseada na quantidade de barras
altura = max(400, len(taxa_template) * 25)

# Cria gráfico de barras horizontal com rótulos
fig2 = go.Figure(go.Bar(
    x=taxa_template['taxa_resposta'],
    y=taxa_template['nome_exibicao'],
    orientation='h',
    text=taxa_template['taxa_resposta'].round(1).astype(str) + '%',
    textposition='outside',
    marker=dict(color='rgba(58, 71, 80, 0.6)', line=dict(color='rgba(58, 71, 80, 1.0)', width=1))
))

fig2.update_layout(
    title="Taxa de Resposta por Template (Nome Exibição)",
    xaxis_title="Taxa de Resposta (%)",
    yaxis_title="Template",
    bargap=0.3,  # Espaço entre as barras
    height=altura,
    margin=dict(l=200, r=20, t=50, b=40),  # Mais espaço para nomes longos
    xaxis=dict(range=[0, taxa_template['taxa_resposta'].max() * 1.1])  # Dá um espaço à direita para texto
)


st.plotly_chart(fig2, use_container_width=True)








# -------------------------------
# Gráfico 3: Taxa de resposta semanal
# -------------------------------
# 1. Adiciona coluna de data (dia específico)
df_filtrado['data'] = df_filtrado['created_at'].dt.date

# 2. Cálculo da taxa de resposta diária
taxa_diaria = df_filtrado.groupby(['data', 'nome_exibicao']).apply(
    lambda x: (x['categoria'] == 'resposta').sum() / (x['categoria'] == 'envio').sum() * 100
    if (x['categoria'] == 'envio').sum() > 0 else 0
).reset_index(name='taxa_resposta')

# 3. Limita a taxa a 100%
taxa_diaria = taxa_diaria[taxa_diaria['taxa_resposta'] <= 100]

# 4. Filtra apenas os dados onde a taxa de resposta é maior que 0
taxa_diaria = taxa_diaria[taxa_diaria['taxa_resposta'] > 0]

# 5. Cores fixas por template
nomes = sorted(taxa_diaria['nome_exibicao'].unique())
paleta = px.colors.qualitative.Set2 + px.colors.qualitative.Set1
cores = {nome: paleta[i % len(paleta)] for i, nome in enumerate(nomes)}

# 6. Cria o gráfico
fig3 = go.Figure()
for nome in nomes:
    df_temp = taxa_diaria[taxa_diaria['nome_exibicao'] == nome]
    fig3.add_trace(go.Scatter(
        x=df_temp['data'],
        y=df_temp['taxa_resposta'],
        mode='lines+markers',
        name=nome,
        line=dict(color=cores[nome]),
        hovertemplate=( 
            "<b>Template:</b> " + nome + "<br>" +
            "<b>Data:</b> %{x|%d/%m/%Y}<br>" +
            "<b>Taxa de Resposta:</b> %{y:.2f}%<extra></extra>"
        )
    ))

# 7. Layout
fig3.update_layout(
    title='',  # Remova ou ajuste conforme preferir
    height=500,
    xaxis=dict(
        title="Data",
        tickformat="%d/%m",
        type='date'
    ),
    yaxis=dict(
        title="Taxa de Resposta (%)",
        range=[0, 100]
    ),
    legend=dict(
        orientation="v",
        yanchor="top",
        y=1,
        xanchor="left",
        x=1.02
    ),
    margin=dict(l=40, r=140, t=20, b=60)
)
st.plotly_chart(fig3, use_container_width=True)
