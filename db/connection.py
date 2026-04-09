from sqlalchemy import create_engine
import streamlit as st

def get_connection():
    """Estabelece conexão com PostgreSQL usando st.secrets"""
    conn_params = st.secrets["postgres"]
    db_url = f"postgresql://{conn_params['user']}:{conn_params['password']}@{conn_params['host']}:{conn_params['port']}/{conn_params['database']}"
    engine = create_engine(db_url, connect_args={"options": "-csearch_path=public,adaptabrasil"})
    return engine.connect()