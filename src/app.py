import streamlit as st
import pandas as pd

# Título da aplicação
st.title("Meu Primeiro App Streamlit 🚀")

# Texto simples
st.write("Aqui está um exemplo simples de como exibir dados.")

# Criando um DataFrame de exemplo
data = pd.DataFrame({
    'Coluna A': [1, 2, 3, 4],
    'Coluna B': [10, 20, 30, 40]
})

# Exibindo uma tabela
st.write("### Tabela de Dados")
st.dataframe(data)

# Exibindo um gráfico de linha simples
st.write("### Gráfico de Linha")
st.line_chart(data)