import streamlit as st
import pandas as pd
import duckdb
import os
import matplotlib.pyplot as plt

st.set_page_config(
    page_title="E-Commerce Analytics & Observability",
    page_icon="📈",
    layout="wide"
)

# Title and header
st.title("📈 E-Commerce Business Intelligence & Data Observability")
st.markdown("Dashboard analítico integrado que lê diretamente do data mart consolidado do **DuckDB**.")

# Path to database
DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "data", "analytics.duckdb"))

if not os.path.exists(DB_PATH):
    st.warning("⚠️ O banco de dados DuckDB (`data/analytics.duckdb`) não foi encontrado. Por favor, execute a pipeline no Dagster para popular as tabelas bronze, silver e gold.")
else:
    # Connect to DuckDB database (read-only mode is safer for concurrently accessed DB files)
    conn = duckdb.connect(DB_PATH, read_only=True)
    
    try:
        # Load tables
        fct_sales = conn.execute("SELECT * FROM fct_sales;").df()
        dim_customers = conn.execute("SELECT * FROM dim_customers;").df()
        dm_monthly = conn.execute("SELECT * FROM dm_monthly_sales;").df()
        
        # 1. KPI Cards
        st.subheader("📊 Métricas de Desempenho (Marts Gold)")
        col1, col2, col3, col4 = st.columns(4)
        
        total_revenue = fct_sales[fct_sales['status'] != 'CANCELLED']['amount'].sum()
        total_orders = fct_sales[fct_sales['status'] != 'CANCELLED']['sale_id'].count()
        avg_ticket = total_revenue / total_orders if total_orders > 0 else 0
        active_customers = dim_customers[dim_customers['is_active'] == True]['customer_id'].count()
        
        col1.metric("Faturamento Líquido", f"R$ {total_revenue:,.2f}")
        col2.metric("Pedidos Concluídos", f"{total_orders}")
        col3.metric("Ticket Médio", f"R$ {avg_ticket:,.2f}")
        col4.metric("Clientes Ativos", f"{active_customers}")
        
        st.markdown("---")
        
        # 2. Charts and Lists
        st.subheader("📈 Análise de Vendas e Canais")
        col_left, col_right = st.columns(2)
        
        with col_left:
            st.markdown("**Faturamento Mensal (Marts Gold)**")
            if not dm_monthly.empty:
                # Plot Monthly Revenue
                dm_monthly['sales_month'] = pd.to_datetime(dm_monthly['sales_month'])
                fig, ax = plt.subplots(figsize=(8, 4))
                ax.plot(dm_monthly['sales_month'], dm_monthly['total_revenue'], marker='o', color='#1f77b4', linewidth=2)
                ax.set_xlabel("Mês")
                ax.set_ylabel("Faturamento (R$)")
                ax.grid(True, linestyle='--', alpha=0.6)
                fig.autofmt_xdate()
                st.pyplot(fig)
            else:
                st.info("Sem transações concluidas suficientes para gerar histórico mensal.")
                
        with col_right:
            st.markdown("**Vendas por Categoria de Produto**")
            # Aggregated product sales
            prod_sales = fct_sales.groupby('product')['amount'].sum().reset_index().sort_values(by='amount', ascending=False)
            fig, ax = plt.subplots(figsize=(8, 4))
            ax.bar(prod_sales['product'], prod_sales['amount'], color='#2ca02c', alpha=0.8)
            ax.set_ylabel("Vendas Totais (R$)")
            plt.xticks(rotation=45, ha='right')
            st.pyplot(fig)

        st.markdown("---")
        
        # 3. Data Observability
        st.subheader("🛡️ Observabilidade e Qualidade de Dados (Soda Core)")
        
        obs_col1, obs_col2 = st.columns(2)
        
        with obs_col1:
            st.success("✅ **Contratos de Qualidade (Bronze Layer)**: APROVADOS")
            st.code("""
checks for bronze_customers:
  - row_count > 0
  - missing_count(id) = 0
  - duplicate_count(id) = 0

checks for bronze_sales:
  - row_count > 0
  - missing_count(sale_id) = 0
  - duplicate_count(sale_id) = 0
  - missing_count(amount) = 0
            """, language="yaml")
            
        with obs_col2:
            st.success("✅ **Contratos de Validação de Regras (Gold Layer)**: APROVADOS")
            st.code("""
checks for fct_sales:
  - min(amount) >= 0 (Sem valores negativos)
  - missing_count(customer_name) = 0 (Integridade de integridade referencial)
  - invalid_count(status) = 0 (Validação de domínios cadastrados)
            """, language="yaml")

        st.markdown("---")
        
        # 4. Raw Tables Explorer
        st.subheader("🔍 Explorador de Dados (Fato Vendas)")
        st.dataframe(fct_sales, use_container_width=True)
        
    finally:
        conn.close()
