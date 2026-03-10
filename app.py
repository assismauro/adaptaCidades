import streamlit as st
import pandas as pd
import pydeck as pdk
import json
from database.connection import get_connection

# Configuração da página
st.set_page_config(
    page_title="Painel Municipal",
    page_icon="🗺️",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# CSS adicional
st.markdown("""
<style>
    [data-testid="collapsedControl"] {
        display: none;
    }
    .stApp > header {
        display: none;
    }
    /* Aumenta o espaçamento horizontal entre as duas colunas */
    div[data-testid="stHorizontalBlock"] {
        column-gap: 100px !important;
    }
    /* Tooltip customizado */
    .tooltip-cell {
        position: relative;
        cursor: pointer;
    }
    .tooltip-cell:hover::after {
        content: attr(data-tooltip);
        position: absolute;
        top: 100%;
        left: 0;
        background-color: #2d2d2d;
        color: #ffffff;
        padding: 8px 14px;
        border-radius: 12px;
        font-family: 'Courier New', monospace;
        font-size: 14px;
        font-weight: normal;
        white-space: nowrap;
        z-index: 9999;
        pointer-events: none;
        margin-top: 8px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.3);
        border: 1px solid #555;
        letter-spacing: 0.3px;
    }
    .tooltip-cell:hover::before {
        content: '';
        position: absolute;
        top: 100%;
        left: 10px;
        transform: translateY(-100%);
        border-width: 6px;
        border-style: solid;
        border-color: transparent transparent #2d2d2d transparent;
        margin-top: -6px;
        z-index: 9999;
        pointer-events: none;
    }
</style>
""", unsafe_allow_html=True)

st.title("Painel Municipal")
st.markdown("---")

# -------------------------------------------------------------------
# Funções de carregamento de dados com cache
# -------------------------------------------------------------------
@st.cache_data(ttl=600)
def load_municipios():
    query = """
    SELECT id, CONCAT(name, ' - ', state) AS display
    FROM adaptabrasil.county
    ORDER BY display;
    """
    try:
        conn = get_connection()
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Erro ao carregar municípios: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=600)
def load_city_geojson(cidade_id):
    query = f"""
    SELECT 
        id, 
        name, 
        state, 
        ST_AsGeoJSON(geom) AS geojson,
        ST_Y(ST_Centroid(geom)) AS latitude,
        ST_X(ST_Centroid(geom)) AS longitude
    FROM adaptabrasil.county
    WHERE id = {cidade_id};
    """
    try:
        conn = get_connection()
        df = pd.read_sql(query, conn)
        conn.close()
        if df.empty:
            return [], None, None
        row = df.iloc[0]
        if row['geojson'] is None:
            return [], None, None
        geom = json.loads(row['geojson'])
        feature = {
            "type": "Feature",
            "geometry": geom,
            "properties": {
                "id": int(row['id']),
                "name": str(row['name']),
                "state": str(row['state'])
            }
        }
        return [feature], float(row['latitude']), float(row['longitude'])
    except Exception as e:
        st.error(f"Erro ao carregar geometria da cidade: {e}")
        return [], None, None

@st.cache_data(ttl=600)
def load_county_data_view(cidade_id):
    query = f"""
    SELECT sep, imageurl, color, value
    FROM adaptabrasil.mv_adapta_cidades
    WHERE county_id = {cidade_id}
    ORDER BY value DESC;
    """
    try:
        conn = get_connection()
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Erro ao carregar dados da view: {e}")
        return pd.DataFrame()

# -------------------------------------------------------------------
# Carregar lista de municípios
# -------------------------------------------------------------------
with st.spinner("Carregando municípios..."):
    df_municipios = load_municipios()

# -------------------------------------------------------------------
# Layout em duas colunas
# -------------------------------------------------------------------
col_esquerda, col_direita = st.columns([1, 1], gap="large")

with col_esquerda:
    if not df_municipios.empty:
        opcoes = df_municipios['display'].tolist()
        selected_display = st.selectbox(
            label="Selecione uma cidade",
            options=opcoes,
            index=None,
            placeholder="Digite o nome da cidade",
            label_visibility="collapsed"
        )
    else:
        st.warning("Lista de municípios não disponível.")
        selected_display = None

    if selected_display:
        cidade_id = df_municipios[df_municipios['display'] == selected_display]['id'].values[0]
        with st.spinner("Carregando mapa da cidade..."):
            cidade_features, lat, lon = load_city_geojson(cidade_id)

        if cidade_features and lat is not None and lon is not None:
            geojson_layer = pdk.Layer(
                "GeoJsonLayer",
                data=cidade_features,
                opacity=0.8,
                stroked=True,
                filled=True,
                extruded=False,
                get_fill_color="[0, 150, 0, 150]",
                get_line_color=[255, 255, 255],
                get_line_width=50,
                pickable=True,
                auto_highlight=True,
                tooltip={
                    "html": "<b>{name}</b> - {state}",
                    "style": {"backgroundColor": "steelblue", "color": "white"}
                }
            )

            view_state = pdk.ViewState(
                latitude=lat,
                longitude=lon,
                zoom=10,
                pitch=0,
                bearing=0
            )

            deck = pdk.Deck(
                layers=[geojson_layer],
                initial_view_state=view_state,
                map_style="mapbox://styles/mapbox/light-v10",
                tooltip={"text": "{name}"}
            )

            st.pydeck_chart(deck, use_container_width=True, height=400)
        else:
            st.warning("Geometria da cidade não disponível.")

with col_direita:
    if selected_display:
        cidade_id = df_municipios[df_municipios['display'] == selected_display]['id'].values[0]
        with st.spinner("Carregando indicadores..."):
            df_dados = load_county_data_view(cidade_id)

        if not df_dados.empty:
            # Tabela com padding-bottom extra para tooltips
            html = """
            <div style='max-height: 450px; overflow-y: auto; font-family: sans-serif; margin-top: 20px; padding-bottom: 50px;'>
                <table style='border-collapse: collapse; border: 0; margin-right: auto;'>
            """
            for _, row in df_dados.iterrows():
                html += "<tr style='border: 0;'>"
                # Ícone com padding direito e classe tooltip-cell
                html += f"<td style='padding: 5px 15px 5px 5px; text-align: center; border: none;' class='tooltip-cell' data-tooltip='{row['sep']}'><img src='{row['imageurl']}' width='32' height='32'></td>"
                # Valor com padding interno e classe tooltip-cell
                html += f"<td style='padding: 5px 5px 5px 10px; text-align: left; font-family: \"Courier New\", monospace; font-weight: bold; background-color: {row['color']}; border-radius: 4px; line-height: 32px; border: none;' class='tooltip-cell' data-tooltip='{row['sep']}'>{row['value']:.3f}</td>"
                html += "</tr>"
            html += """
                </table>
            </div>
            """
            st.markdown(html, unsafe_allow_html=True)
        else:
            st.info("Nenhum dado disponível para este município.")
    else:
        st.info("Selecione um município para visualizar os indicadores.")

# Rodapé
st.markdown("---")
st.caption("Fonte: adaptabrasil")