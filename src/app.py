import streamlit as st
import pandas as pd
import plotly.express as px
from pymongo import MongoClient
import certifi
import urllib.parse

st.set_page_config(page_title="Dashboard de Franquias (NoSQL)",
                   page_icon="🍃",
                   layout="wide")

@st.cache_resource
def init_connection():
    """Tenta conectar usando formato novo (username/password) ou antigo (uri)."""
    try:
        mongo_secrets = st.secrets["mongo"]
        
        if "username" in mongo_secrets and "password" in mongo_secrets:
            username = urllib.parse.quote_plus(mongo_secrets["username"])
            password = urllib.parse.quote_plus(mongo_secrets["password"])
            host = mongo_secrets["host"]
            uri = f"mongodb+srv://{username}:{password}@{host}/?retryWrites=true&w=majority"
        
        elif "uri" in mongo_secrets:
            uri = mongo_secrets["uri"]
        
        else:
            st.error("Erro no secrets.toml: Defina 'username' e 'password' OU 'uri'.")
            return None

        return MongoClient(uri, tlsCAFile=certifi.where())
    except Exception as e:
        st.error(f"Erro de Conexão: {e}")
        return None

client = init_connection()
if client:
    db_name = st.secrets["mongo"].get("db", "test")
    db = client[db_name]
else:
    db = None

def normalizar_df(dados_list):
    """Converte lista do Mongo em DF e garante coluna 'id' como string."""
    if not dados_list:
        return pd.DataFrame()
    
    df = pd.DataFrame(dados_list)
    
    if 'id' not in df.columns and '_id' in df.columns:
        df['id'] = df['_id'].astype(str)
    
    if 'id' in df.columns:
        df['id'] = df['id'].astype(str)
        
    return df

def limpar_visualizacao(df):
    """Remove colunas técnicas antes de exibir na tabela."""
    if df.empty: return df
    
    df_view = df.copy()

    for col in df_view.columns:
        primeiro_valor = df_view[col].dropna().iloc[0] if not df_view[col].dropna().empty else None
        
        if isinstance(primeiro_valor, dict):
            df_view[col] = df_view[col].apply(lambda x: 
                " | ".join([f"{k.replace('_', ' ').title()}: {v}" 
                           for k, v in x.items() 
                           if 'id' not in k.lower() and '_id' not in k.lower()]) 
                if isinstance(x, dict) else ""
            )
    
    colunas_remover = [
        'id',
        '_id',              
        'franquia_id',      
        'comandante_id',   
    ]
    
    cols_to_drop = [c for c in df_view.columns if c in colunas_remover or c.endswith('_str')]
    return df_view.drop(columns=cols_to_drop)


@st.cache_data(ttl=600)
def carregar_franquias():
    if db is None: return pd.DataFrame(columns=['id', 'nome'])
    try:
        cursor = db.franquias.find({}, {'nome': 1, 'id': 1, '_id': 1})
        df = normalizar_df(list(cursor))
        
        if df.empty: return pd.DataFrame(columns=['id', 'nome'])
        
        if 'nome' not in df.columns: df['nome'] = "Sem Nome"
        if 'id' not in df.columns: df['id'] = df.index.astype(str)
            
        return df[['id', 'nome']].sort_values('nome')
    except Exception as e:
        st.error(f"Erro ao carregar franquias: {e}")
        return pd.DataFrame(columns=['id', 'nome'])

@st.cache_data(ttl=600)
def carregar_dados_franquia(franquia_id):
    if db is None: return {"orgs": pd.DataFrame(), "inds": pd.DataFrame(), "veis": pd.DataFrame()}

    try:
        filtro = {"$or": [
            {"id_franquia": franquia_id}, 
            {"id_franquia": str(franquia_id)},
            {"id_franquia": int(franquia_id) if str(franquia_id).isdigit() else -1}
        ]}
        
        orgs = list(db.organizacoes.find(filtro))
        inds = list(db.individuos.find(filtro))
        veiculos = list(db.veiculos.find(filtro))
        
        df_orgs = normalizar_df(orgs)
        df_inds = normalizar_df(inds)
        df_veis = normalizar_df(veiculos)

        if not df_veis.empty:
            comandantes = list(db.comandantes.find({}))
            todos_inds = list(db.individuos.find({}, {'id': 1, 'nome': 1, '_id': 1}))
            
            df_cmd = normalizar_df(comandantes)
            df_todos_inds = normalizar_df(todos_inds)

            if not df_cmd.empty and not df_todos_inds.empty:
                df_veis['id_cmd_str'] = df_veis['id_comandante'].astype(str) if 'id_comandante' in df_veis.columns else ''
                df_cmd['id_str'] = df_cmd['id'].astype(str)
                
                df_veis = pd.merge(df_veis, df_cmd, left_on='id_cmd_str', right_on='id_str', how='left', suffixes=('', '_cmd_info'))
                
                col_id_ind = 'id_individuo' if 'id_individuo' in df_cmd.columns else 'id_individuo_cmd_info'
                
                if col_id_ind in df_veis.columns:
                    df_veis['id_ind_final'] = df_veis[col_id_ind].astype(str)
                    df_todos_inds['id_str'] = df_todos_inds['id'].astype(str)
                    
                    final = pd.merge(df_veis, df_todos_inds, left_on='id_ind_final', right_on='id_str', how='left', suffixes=('', '_nome_final'))
                    
                    if 'nome_nome_final' in final.columns:
                        df_veis['comandante_nome'] = final['nome_nome_final']
                    elif 'nome' in final.columns:
                         pass 
                    
                    cols_nome = [c for c in final.columns if 'nome' in c and c != 'nome']
                    if cols_nome:
                        df_veis['comandante_nome'] = final[cols_nome[-1]]
                    else:
                        df_veis['comandante_nome'] = "Desconhecido"
                else:
                    df_veis['comandante_nome'] = "Desconhecido"

        return {"orgs": df_orgs, "inds": df_inds, "veis": df_veis}
    except Exception as e:
        st.error(f"Erro ao carregar dados detalhados: {e}")
        return {"orgs": pd.DataFrame(), "inds": pd.DataFrame(), "veis": pd.DataFrame()}

@st.cache_data(ttl=600)
def carregar_todos_os_dados():
    if db is None: return {"orgs": pd.DataFrame(), "inds": pd.DataFrame(), "veis": pd.DataFrame()}
    try:
        df_franquias = normalizar_df(list(db.franquias.find({})))
        df_orgs = normalizar_df(list(db.organizacoes.find({})))
        df_inds = normalizar_df(list(db.individuos.find({})))
        df_veis = normalizar_df(list(db.veiculos.find({})))
        
        if df_franquias.empty:
             return {"orgs": df_orgs, "inds": df_inds, "veis": df_veis}

        df_franquias['id_str'] = df_franquias['id'].astype(str)
        for df in [df_orgs, df_inds, df_veis]:
            if not df.empty and 'id_franquia' in df.columns:
                df['id_franquia_str'] = df['id_franquia'].astype(str)
                temp = pd.merge(df, df_franquias[['id_str', 'nome']], left_on='id_franquia_str', right_on='id_str', how='left')
                df['nome_franquia'] = temp['nome']

        return {"orgs": df_orgs, "inds": df_inds, "veis": df_veis}
    except Exception as e:
        st.error(f"Erro geral: {e}")
        return {"orgs": pd.DataFrame(), "inds": pd.DataFrame(), "veis": pd.DataFrame()}

st.title("🍃 Dashboard Interativo de Veículos (MongoDB)")
st.markdown("---")

df_franquias = carregar_franquias()

st.sidebar.header("Filtro Principal")

if df_franquias.empty:
    st.sidebar.warning("Nenhuma franquia carregada. Verifique a conexão.")
    dados = {"orgs": pd.DataFrame(), "inds": pd.DataFrame(), "veis": pd.DataFrame()}
else:
    try:
        opcoes = ["Todas as Franquias"] + df_franquias['nome'].tolist()
        mapa_ids = dict(zip(df_franquias['nome'], df_franquias['id']))
        
        escolha = st.sidebar.selectbox("Selecione:", options=opcoes)
        
        if escolha == "Todas as Franquias":
            dados = carregar_todos_os_dados()
            st.header(f"Visão Geral: {escolha}")
        else:
            id_escolhido = mapa_ids.get(escolha)
            dados = carregar_dados_franquia(id_escolhido)
            st.header(f"Visão Geral: {escolha}")
    except Exception as e:
        st.sidebar.error(f"Erro ao criar menu: {e}")
        dados = {"orgs": pd.DataFrame(), "inds": pd.DataFrame(), "veis": pd.DataFrame()}

df_orgs = dados["orgs"]
df_inds = dados["inds"]
df_veis = dados["veis"]

st.sidebar.markdown("---")
st.sidebar.header("Filtros Detalhados")

if not df_orgs.empty and 'tipo_organizacao' in df_orgs.columns:
    tipos = df_orgs['tipo_organizacao'].unique()
    sel = st.sidebar.multiselect("Tipo Org:", tipos, default=tipos)
    df_orgs = df_orgs[df_orgs['tipo_organizacao'].isin(sel)]

if not df_inds.empty and 'especie' in df_inds.columns:
    esps = df_inds['especie'].unique()
    sel = st.sidebar.multiselect("Espécie:", esps, default=esps)
    df_inds = df_inds[df_inds['especie'].isin(sel)]

if not df_veis.empty and 'fabricante' in df_veis.columns:
    df_veis['fabricante'] = df_veis['fabricante'].fillna("Desconhecido")
    fabs = df_veis['fabricante'].unique()
    sel = st.sidebar.multiselect("Fabricante:", fabs, default=fabs)
    df_veis = df_veis[df_veis['fabricante'].isin(sel)]

col1, col2, col3 = st.columns(3)
col1.metric("Organizações", len(df_orgs))
col2.metric("Indivíduos", len(df_inds))
col3.metric("Veículos", len(df_veis))

st.markdown("---")
t1, t2, t3 = st.tabs(["🏢 Organizações", "👥 Indivíduos", "🚀 Veículos"])

with t1:
    if not df_orgs.empty:
        st.plotly_chart(px.pie(df_orgs, names='tipo_organizacao'), use_container_width=True)
        st.dataframe(limpar_visualizacao(df_orgs), hide_index=True)
    else: st.info("Sem dados.")

with t2:
    if not df_inds.empty:
        st.plotly_chart(px.bar(df_inds, x='especie'), use_container_width=True)
        st.dataframe(limpar_visualizacao(df_inds), hide_index=True)
    else: st.info("Sem dados.")

with t3:
    if not df_veis.empty:
        st.plotly_chart(px.bar(df_veis, x='fabricante'), use_container_width=True)
        st.dataframe(limpar_visualizacao(df_veis), hide_index=True)
    else: st.info("Sem dados.")