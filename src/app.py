import streamlit as st
import pandas as pd
import plotly.express as px
from pymongo import MongoClient
import certifi
import urllib.parse

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(page_title="Dashboard de Franquias (NoSQL)",
                   page_icon="🍃",
                   layout="wide")

# --- FUNÇÃO DE CONEXÃO MONGO ---
@st.cache_resource
def init_connection():
    try:
        # Tratamento para caracteres especiais na senha
        username = urllib.parse.quote_plus(st.secrets["mongo"]["username"])
        password = urllib.parse.quote_plus(st.secrets["mongo"]["password"])
        host = st.secrets["mongo"]["host"]
        
        uri = f"mongodb+srv://{username}:{password}@{host}/?retryWrites=true&w=majority"
        return MongoClient(uri, tlsCAFile=certifi.where())
    except Exception as e:
        st.error(f"Erro ao conectar ao MongoDB: {e}")
        return None

client = init_connection()
db = client[st.secrets["mongo"]["db"]] if client else None

# --- FUNÇÃO AUXILIAR PARA NORMALIZAR IDS ---
def normalizar_df(dados_list):
    """Converte lista do Mongo em DF e garante que existe uma coluna 'id'."""
    if not dados_list:
        return pd.DataFrame()
    
    df = pd.DataFrame(dados_list)
    
    # Se não existir a coluna 'id' (do SQL), criamos usando o '_id' do Mongo
    if 'id' not in df.columns and '_id' in df.columns:
        df['id'] = df['_id'].astype(str)
    
    # Se existir 'id' mas for numérico e quisermos garantir compatibilidade, mantemos.
    # Mas se houver mistura, forçamos string.
    
    return df

# --- FUNÇÕES DE CONSULTA (COM CACHE) ---

@st.cache_data(ttl=600)
def carregar_franquias():
    """Carrega a lista de franquias de forma segura."""
    if db is None: return pd.DataFrame(columns=['id', 'nome'])
    
    try:
        # Trazemos o _id e o id (se existir)
        cursor = db.franquias.find({}, {'nome': 1, 'id': 1, '_id': 1})
        df = normalizar_df(list(cursor))
        
        if df.empty: return pd.DataFrame(columns=['id', 'nome'])
        
        # Garante colunas mínimas
        if 'nome' not in df.columns: df['nome'] = "Sem Nome"
        if 'id' not in df.columns: df['id'] = df.index.astype(str) # Fallback extremo
            
        return df[['id', 'nome']].sort_values('nome')
    except Exception as e:
        st.error(f"Erro ao carregar franquias: {e}")
        return pd.DataFrame(columns=['id', 'nome'])

@st.cache_data(ttl=600)
def carregar_dados_franquia(franquia_id):
    """Carrega dados filtrados por ID da franquia."""
    if db is None: return {"orgs": pd.DataFrame(), "inds": pd.DataFrame(), "veis": pd.DataFrame()}

    try:
        # Tenta buscar pelo ID numérico (SQL antigo) OU pelo ID string (Mongo novo)
        # Isso garante que funcione independente de como o dado foi migrado
        filtro = {"$or": [{"id_franquia": franquia_id}, {"id_franquia": str(franquia_id)}, {"id_franquia": int(franquia_id) if str(franquia_id).isdigit() else -1}]}
        
        orgs = list(db.organizacoes.find(filtro))
        inds = list(db.individuos.find(filtro))
        veiculos = list(db.veiculos.find(filtro))
        
        df_orgs = normalizar_df(orgs)
        df_inds = normalizar_df(inds)
        df_veis = normalizar_df(veiculos)

        # Lógica de JOIN para Veículos -> Comandantes
        if not df_veis.empty:
            comandantes = list(db.comandantes.find({}))
            todos_inds = list(db.individuos.find({}, {'id': 1, '_id': 1, 'nome': 1}))
            
            df_cmd = normalizar_df(comandantes)
            df_todos_inds = normalizar_df(todos_inds)

            if not df_cmd.empty and not df_todos_inds.empty:
                # Merge seguro convertendo chaves para mesmo tipo (string)
                df_veis['id_comandante_str'] = df_veis['id_comandante'].astype(str)
                df_cmd['id_str'] = df_cmd['id'].astype(str)
                
                df_veis = pd.merge(df_veis, df_cmd, left_on='id_comandante_str', right_on='id_str', how='left', suffixes=('', '_cmd'))
                
                # Merge para nome
                if 'id_individuo' in df_cmd.columns:
                     # O id_individuo veio do merge anterior, agora no df_veis
                     # Atenção: colisão de nomes. O id_individuo do comandante deve ser usado.
                     col_id_ind = 'id_individuo' if 'id_individuo' in df_veis.columns else 'id_individuo_cmd'
                     
                     df_veis['id_ind_cmd_str'] = df_veis[col_id_ind].astype(str)
                     df_todos_inds['id_str'] = df_todos_inds['id'].astype(str)
                     
                     df_veis = pd.merge(df_veis, df_todos_inds, left_on='id_ind_cmd_str', right_on='id_str', how='left', suffixes=('', '_nome_cmd'))
                     
                     if 'nome' in df_veis.columns:
                         df_veis.rename(columns={'nome': 'comandante_nome'}, inplace=True)
                     elif 'nome_nome_cmd' in df_veis.columns:
                         df_veis.rename(columns={'nome_nome_cmd': 'comandante_nome'}, inplace=True)

        return {"orgs": df_orgs, "inds": df_inds, "veis": df_veis}
    except Exception as e:
        st.error(f"Erro ao carregar dados detalhados: {e}")
        return {"orgs": pd.DataFrame(), "inds": pd.DataFrame(), "veis": pd.DataFrame()}

@st.cache_data(ttl=600)
def carregar_todos_os_dados():
    """Carrega TODOS os dados com tratamento de colunas."""
    if db is None: return {"orgs": pd.DataFrame(), "inds": pd.DataFrame(), "veis": pd.DataFrame()}
    
    try:
        # Carrega tudo normalizando (garante coluna 'id')
        df_franquias = normalizar_df(list(db.franquias.find({})))
        df_orgs = normalizar_df(list(db.organizacoes.find({})))
        df_inds = normalizar_df(list(db.individuos.find({})))
        df_veis = normalizar_df(list(db.veiculos.find({})))
        df_cmd = normalizar_df(list(db.comandantes.find({})))

        # Se franquias estiver vazio, retorna vazio para evitar erro de coluna
        if df_franquias.empty:
             return {"orgs": df_orgs, "inds": df_inds, "veis": df_veis}

        # Prepara chaves para Join (garante string para bater ID Mongo com ID SQL se misturados)
        df_franquias['id_str'] = df_franquias['id'].astype(str)

        # Merge com Franquias
        for df in [df_orgs, df_inds, df_veis]:
            if not df.empty and 'id_franquia' in df.columns:
                df['id_franquia_str'] = df['id_franquia'].astype(str)
                
                # Merge temporário para pegar o nome
                temp = pd.merge(df, df_franquias[['id_str', 'nome']], left_on='id_franquia_str', right_on='id_str', how='left')
                df['nome_franquia'] = temp['nome']

        # Merge Veículos -> Comandante
        if not df_veis.empty and not df_cmd.empty and not df_inds.empty:
            df_veis['id_cmd_str'] = df_veis['id_comandante'].astype(str)
            df_cmd['id_str'] = df_cmd['id'].astype(str)
            df_inds['id_str'] = df_inds['id'].astype(str)

            # Join Veiculo -> Comandante
            df_merged = pd.merge(df_veis, df_cmd, left_on='id_cmd_str', right_on='id_str', how='left', suffixes=('', '_cmd'))
            
            # Join Resultado -> Individuo (Nome)
            # O id_individuo relevante é o que veio do Comandante
            col_ind = 'id_individuo' if 'id_individuo' in df_cmd.columns else 'id_individuo_cmd'
            # No merge anterior, colunas do comandante podem ter sufixo se conflitaram.
            # Assumindo estrutura padrão:
            if 'id_individuo' in df_merged.columns: # se o veículo não tinha essa coluna, ela veio do comandante
                 df_merged['id_ind_final'] = df_merged['id_individuo'].astype(str)
                 final = pd.merge(df_merged, df_inds[['id_str', 'nome']], left_on='id_ind_final', right_on='id_str', how='left')
                 df_veis['comandante_nome'] = final['nome']
            else:
                 df_veis['comandante_nome'] = "Desconhecido"

        return {"orgs": df_orgs, "inds": df_inds, "veis": df_veis}
    except Exception as e:
        st.error(f"Erro geral ao processar dados: {e}")
        return {"orgs": pd.DataFrame(), "inds": pd.DataFrame(), "veis": pd.DataFrame()}

# --- APLICAÇÃO PRINCIPAL ---
st.title("🍃 Dashboard Interativo de Franquias (MongoDB)")
st.markdown("---")

df_franquias = carregar_franquias()

# --- SIDEBAR ROBUSTA ---
st.sidebar.header("Filtro Principal")

if df_franquias.empty:
    st.sidebar.warning("Nenhuma franquia carregada.")
    dados = {"orgs": pd.DataFrame(), "inds": pd.DataFrame(), "veis": pd.DataFrame()}
else:
    # Garante que temos valores únicos e converte para dicionário
    try:
        franquia_map = dict(zip(df_franquias['nome'], df_franquias['id']))
        opcoes = ["Todas as Franquias"] + list(franquia_map.keys())
        
        escolha = st.sidebar.selectbox("Selecione:", options=opcoes)
        
        if escolha == "Todas as Franquias":
            dados = carregar_todos_os_dados()
            st.header(f"Visão Geral: {escolha}")
        else:
            id_escolhido = franquia_map[escolha]
            dados = carregar_dados_franquia(id_escolhido)
            st.header(f"Visão Geral: {escolha}")
            
    except Exception as e:
        st.sidebar.error(f"Erro na lista de seleção: {e}")
        dados = {"orgs": pd.DataFrame(), "inds": pd.DataFrame(), "veis": pd.DataFrame()}

# --- EXIBIÇÃO DOS DADOS (CÓDIGO UI MANTIDO IGUAL) ---
df_orgs = dados["orgs"]
df_inds = dados["inds"]
df_veis = dados["veis"]

# Filtros da Sidebar (Só mostra se tiver dados)
st.sidebar.markdown("---")
st.sidebar.header("Filtros Detalhados")

# Filtro Org
if not df_orgs.empty and 'tipo_organizacao' in df_orgs.columns:
    tipos = df_orgs['tipo_organizacao'].dropna().unique()
    sel_tipos = st.sidebar.multiselect("Tipo de Organização:", tipos, default=tipos)
    df_orgs = df_orgs[df_orgs['tipo_organizacao'].isin(sel_tipos)]

# Filtro Indivíduos
if not df_inds.empty and 'especie' in df_inds.columns:
    especies = df_inds['especie'].dropna().unique()
    sel_esp = st.sidebar.multiselect("Espécie:", especies, default=especies)
    df_inds = df_inds[df_inds['especie'].isin(sel_esp)]

# Filtro Veículos
if not df_veis.empty and 'fabricante' in df_veis.columns:
    df_veis['fabricante'] = df_veis['fabricante'].fillna("Desconhecido")
    fabs = df_veis['fabricante'].unique()
    sel_fabs = st.sidebar.multiselect("Fabricante:", fabs, default=fabs)
    df_veis = df_veis[df_veis['fabricante'].isin(sel_fabs)]

# Métricas
col1, col2, col3 = st.columns(3)
col1.metric("Organizações", len(df_orgs))
col2.metric("Indivíduos", len(df_inds))
col3.metric("Veículos", len(df_veis))

st.markdown("---")
tab1, tab2, tab3 = st.tabs(["🏢 Organizações", "👥 Indivíduos", "🚀 Veículos"])

with tab1:
    if not df_orgs.empty:
        fig = px.pie(df_orgs, names='tipo_organizacao', title="Distribuição por Tipo")
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(df_orgs)
    else: st.info("Sem dados de organizações.")

with tab2:
    if not df_inds.empty:
        fig = px.bar(df_inds, x='especie', title="Contagem por Espécie")
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(df_inds)
    else: st.info("Sem dados de indivíduos.")

with tab3:
    if not df_veis.empty:
        fig = px.bar(df_veis, x='fabricante', title="Veículos por Fabricante")
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(df_veis)
    else: st.info("Sem dados de veículos.")