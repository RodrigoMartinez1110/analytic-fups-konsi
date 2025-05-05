import time
import streamlit as st
from pymongo import MongoClient
import pandas as pd
import re
import plotly.express as px

# -------------------------------
# Função de atualização a cada 10 minutos
# -------------------------------
def auto_update():
    # Intervalo de atualização em segundos (10 minutos)
    intervalo = 10 * 60  # 10 minutos em segundos

    # Registrar a hora atual
    agora = time.time()
    
    # Tempo até a próxima execução
    tempo_ate_proxima_execucao = intervalo - (agora % intervalo)
    
    # Aguardar o tempo até a próxima execução
    time.sleep(tempo_ate_proxima_execucao)
    
    # Forçar o Streamlit a atualizar a página
    st.experimental_rerun()

# -------------------------------
# Conexão com o MongoDB
# -------------------------------
uri = st.secrets["mongodb"]["uri"]
client = MongoClient(uri)
db = client["growth"]
collection = db["events"]

# -------------------------------
# Carregamento e filtragem inicial dos dados
# -------------------------------
dados = list(collection.find())
df = pd.DataFrame(dados)
df['created_at'] = pd.to_datetime(df['created_at'])

# Filtros de data e horário no Streamlit
data_min = df['created_at'].min().date()
data_max = df['created_at'].max().date()
data_inicio = st.sidebar.date_input("Data inicial", value=data_min, min_value=data_min, max_value=data_max)
data_fim = st.sidebar.date_input("Data final", value=data_max, min_value=data_min, max_value=data_max)
df_filtrado = df[(df['created_at'].dt.date >= data_inicio) & (df['created_at'].dt.date <= data_fim)]

# Remove eventos irrelevantes
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
    "FUP 1 resposta": "robo_giovanna_leads_ativos_0fup1_ativo_Resposta"
}

# Aplica mapeamento manual de nomes legíveis
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

# Exibir o resumo final
st.write("**Resumo detalhado por template**")

# -------------------------------
# Chama a função de atualização automática
# -------------------------------
auto_update()
