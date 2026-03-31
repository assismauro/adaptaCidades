import streamlit as st
import pandas as pd
import pydeck as pdk
import plotly.express as px
import json
from database.connection import get_connection
from pathlib import Path
import base64
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from io import BytesIO

# Configuração da página
st.set_page_config(
    page_title="Painel Municipal",
    page_icon="🗺️",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# CSS personalizado
st.markdown("""
<style>
    .stApp {
        background: linear-gradient(145deg, #a8d8ea 0%, #f0f0c0 50%, #d2b48c 100%);
        background-attachment: fixed;
    }
    .main > .block-container {
        background-color: rgba(255, 255, 255, 0.85);
        border-radius: 20px;
        padding: 1rem 2rem !important;
        margin-top: 1rem;
        margin-bottom: 1rem;
        box-shadow: 0 4px 20px rgba(0,0,0,0.1);
        backdrop-filter: blur(3px);
    }
    [data-testid="collapsedControl"] {
        display: none;
    }
    .stApp > header {
        display: none;
    }
    div[data-testid="stHorizontalBlock"] {
        column-gap: 100px !important;
    }
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


# -------------------------------------------------------------------
# Funções de carregamento de dados com cache
# -------------------------------------------------------------------
@st.cache_data(ttl=600, show_spinner=False)
def load_municipios():
    query = """
            SELECT id, name, state, CONCAT(name, ' - ', state) AS display
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


@st.cache_data(ttl=600, show_spinner=False)
def load_anos_para_cidade(cidade_id):
    query = f"""
    SELECT DISTINCT "year"
    FROM adaptabrasil.mv_painel_municipal
    WHERE county_id = {cidade_id}
    ORDER BY "year";
    """
    try:
        conn = get_connection()
        df = pd.read_sql(query, conn)
        conn.close()
        return df["year"].tolist()
    except Exception as e:
        st.error(f"Erro ao carregar anos para a cidade: {e}")
        return []


@st.cache_data(ttl=600, show_spinner=False)
def load_setores_para_cidade_ano(cidade_id, ano):
    query = f"""
    SELECT DISTINCT sep
    FROM adaptabrasil.mv_painel_municipal
    WHERE county_id = {cidade_id} AND "year" = '{ano}'
    ORDER BY sep;
    """
    try:
        conn = get_connection()
        df = pd.read_sql(query, conn)
        conn.close()
        return df["sep"].tolist()
    except Exception as e:
        st.error(f"Erro ao carregar setores: {e}")
        return []


@st.cache_data(ttl=600, show_spinner=False)
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


@st.cache_data(ttl=600, show_spinner=False)
def load_county_data_view(cidade_id, ano, sep=None):
    if sep and sep != "Selecione o Setor Estratégico desejado":
        query = f"""
        SELECT sep, imageurl, color, value, label, "order"
        FROM adaptabrasil.mv_painel_municipal
        WHERE county_id = {cidade_id} AND "year" = '{ano}' AND sep = '{sep}'
        ORDER BY value DESC;
        """
    else:
        query = f"""
        SELECT sep, imageurl, color, value, label, "order"
        FROM adaptabrasil.mv_painel_municipal
        WHERE county_id = {cidade_id} AND "year" = '{ano}'
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


@st.cache_data(ttl=600, show_spinner=False)
def load_ranking_data(cidade_id, ano, sep):
    geo_query = f"""
    SELECT county, state, microregion, mesoregion, region
    FROM adaptabrasil.mv_painel_municipal
    WHERE county_id = {cidade_id} AND "year" = '{ano}' AND sep = '{sep}'
    LIMIT 1;
    """
    try:
        conn = get_connection()
        geo_df = pd.read_sql(geo_query, conn)
        if geo_df.empty:
            conn.close()
            return pd.DataFrame()
        county_name = geo_df.iloc[0]['county']
        state = geo_df.iloc[0]['state']
        microregion = geo_df.iloc[0]['microregion']
        mesoregion = geo_df.iloc[0]['mesoregion']
        region = geo_df.iloc[0]['region']
    except Exception as e:
        st.error(f"Erro ao carregar dados geográficos: {e}")
        conn.close()
        return pd.DataFrame()

    query = f"""
    SELECT * FROM (
        SELECT 
            0 as orderby,
            'País' as label,
            'Brasil' as resolucao,
            ranking,
            total_lines
        FROM (
            SELECT 
                county,
                RANK() OVER (ORDER BY value DESC) AS ranking,
                COUNT(*) OVER () AS total_lines
            FROM adaptabrasil.mv_painel_municipal
            WHERE year = '{ano}'
              AND sep = '{sep}'
        ) ranked
        WHERE county = '{county_name}'
        UNION
        SELECT 
            1 as orderby,
            'Estado' as label,
            state as resolucao,
            ranking,
            total_lines
        FROM (
            SELECT 
                state,
                county,
                value,
                RANK() OVER (ORDER BY value DESC) AS ranking,
                COUNT(*) OVER () AS total_lines
            FROM adaptabrasil.mv_painel_municipal
            WHERE year = '{ano}'
              AND sep = '{sep}'
              AND state = '{state}'
        ) ranked
        WHERE county = '{county_name}'
        UNION
        SELECT 
            2 as orderby,
            'Região' as label,
            region as resolucao,
            ranking,
            total_lines
        FROM (
            SELECT 
                region,
                county,
                RANK() OVER (ORDER BY value DESC) AS ranking,
                COUNT(*) OVER () AS total_lines
            FROM adaptabrasil.mv_painel_municipal
            WHERE year = '{ano}' 
              AND sep = '{sep}'
              AND region = '{region}'
        ) ranked
        WHERE county = '{county_name}'
        UNION
        SELECT 
            3 as orderby,
            'Mesorregião' as label,
            mesoregion as resolucao,
            ranking,
            total_lines
        FROM (
            SELECT 
                mesoregion,
                county,
                value,
                RANK() OVER (ORDER BY value DESC) AS ranking,
                COUNT(*) OVER () AS total_lines
            FROM adaptabrasil.mv_painel_municipal
            WHERE year = '{ano}'
              AND sep = '{sep}'
              AND mesoregion = '{mesoregion}'
        ) ranked
        WHERE county = '{county_name}'
        UNION
        SELECT 
            4 as orderby,
            'Microrregião' as label,
            microregion as resolucao,
            ranking,
            total_lines
        FROM (
            SELECT 
                microregion,
                county,
                value,
                RANK() OVER (ORDER BY value DESC) AS ranking,
                COUNT(*) OVER () AS total_lines
            FROM adaptabrasil.mv_painel_municipal
            WHERE year = '{ano}'
              AND sep = '{sep}'
              AND microregion = '{microregion}'
        ) ranked
        WHERE county = '{county_name}'
    ) t
    ORDER BY orderby;
    """
    try:
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Erro ao carregar dados de ranking: {e}")
        conn.close()
        return pd.DataFrame()


# -------------------------------------------------------------------
# Carregar lista de municípios
# -------------------------------------------------------------------
with st.spinner("Carregando municípios..."):
    df_municipios = load_municipios()


# -------------------------------------------------------------------
# Funções auxiliares para o PDF
# -------------------------------------------------------------------
def draw_footer(c, width, height, page_num, total_pages=None):
    c.setStrokeColorRGB(0.5, 0.5, 0.5)
    c.setLineWidth(0.5)
    c.line(50, 50, width - 50, 50)
    c.setFont("Helvetica", 8)
    footer_text = "Documento gerado pelo Painel Municipal"
    c.drawString(50, 35, footer_text)
    if total_pages:
        page_text = f"Página {page_num} de {total_pages}"
    else:
        page_text = f"Página {page_num}"
    c.drawRightString(width - 50, 35, page_text)


# -------------------------------------------------------------------
# Cabeçalho customizado + botão Plano de Adaptação
# -------------------------------------------------------------------
logo_path = Path(__file__).parent / "assets" / "AdaptaLogo.png"


def image_to_base64(image_path):
    with open(image_path, "rb") as img_file:
        return base64.b64encode(img_file.read()).decode()


col_logo, col_title, col_btn = st.columns([0.12, 0.68, 0.20])

with col_logo:
    if logo_path.exists():
        logo_base64 = image_to_base64(logo_path)
        st.markdown(f'<img src="data:image/png;base64,{logo_base64}" width="180">', unsafe_allow_html=True)

with col_title:
    st.markdown("<h1 style='margin: 0; line-height: 1.2;'>Painel Municipal</h1>", unsafe_allow_html=True)

with col_btn:
    selected_city_display = st.session_state.get('cidade_select')
    button_disabled = selected_city_display is None

    # Botão para gerar o PDF
    if st.button("Plano de Adaptação", key="plan_button", disabled=button_disabled):
        with st.spinner("Gerando PDF..."):
            # Capturar os valores diretamente dos widgets
            current_city_display = st.session_state.get('cidade_select')
            current_ano = st.session_state.get('ano_select')
            current_sep = st.session_state.get('sep_select')

            # Obter nome da cidade
            current_city_name = ""
            current_city_state = ""
            if current_city_display and not df_municipios.empty:
                cidade_row = df_municipios[df_municipios['display'] == current_city_display]
                if not cidade_row.empty:
                    current_city_name = cidade_row.iloc[0]['name']
                    current_city_state = cidade_row.iloc[0]['state']

            # Criar o PDF
            buffer = BytesIO()
            c = canvas.Canvas(buffer, pagesize=letter)
            width, height = letter
            page_num = 1

            # Centralizar o título
            c.setFont("Helvetica-Bold", 24)
            title = "Plano de Adaptação"
            title_width = c.stringWidth(title, "Helvetica-Bold", 24)
            c.drawString((width - title_width) / 2, height - 50, title)

            # Município
            y = height - 100
            c.setFont("Helvetica", 14)
            c.drawString(50, y, f"Município: {current_city_name} - {current_city_state}")

            # Ano
            y -= 25
            c.setFont("Helvetica", 12)
            if current_ano:
                ano_display = current_ano.strip() if isinstance(current_ano, str) else str(current_ano)
            else:
                ano_display = "Não selecionado"
            c.drawString(50, y, f"Ano: {ano_display}")

            # Setor Estratégico
            y -= 25
            if current_sep and current_sep != "Selecione o Setor Estratégico desejado":
                c.drawString(50, y, f"Setor Estratégico: {current_sep}")
            else:
                c.drawString(50, y, "Setor Estratégico: Todos os Setores")

            # Adicionar rodapé
            draw_footer(c, width, height, page_num)

            c.save()
            buffer.seek(0)

            # Armazenar o PDF no session_state
            st.session_state['pdf_data'] = buffer.getvalue()
            st.session_state['pdf_filename'] = f"Plano de Adaptação - {current_city_name}-{current_city_state}.pdf"
            st.session_state['pdf_ready'] = True

            st.success("PDF gerado com sucesso! Clique no botão abaixo para baixar.")
            # Removeu o st.rerun() - agora a página não recarrega

    # Mostrar botão de download se o PDF estiver pronto
    if st.session_state.get('pdf_ready', False):
        st.download_button(
            label="Baixar PDF",
            data=st.session_state['pdf_data'],
            file_name=st.session_state['pdf_filename'],
            mime="application/pdf",
            key="pdf_download"
        )

# Inicializar session_state
if 'cidade_id' not in st.session_state:
    st.session_state['cidade_id'] = None
    st.session_state['estado_cidade'] = None
    st.session_state['selected_ano'] = None
    st.session_state['selected_sep'] = None
    st.session_state['pdf_ready'] = False

# -------------------------------------------------------------------
# Layout em duas colunas
# -------------------------------------------------------------------
col_esquerda, col_direita = st.columns([1, 1], gap="large")

with col_esquerda:
    if not df_municipios.empty:
        opcoes_cidades = df_municipios['display'].tolist()
        selected_display = st.selectbox(
            label="Selecione uma cidade",
            options=opcoes_cidades,
            index=None,
            placeholder="Digite o nome da cidade",
            label_visibility="collapsed",
            key="cidade_select"
        )
    else:
        st.warning("Lista de municípios não disponível.")
        selected_display = None

    if selected_display:
        cidade_row = df_municipios[df_municipios['display'] == selected_display].iloc[0]
        st.session_state['cidade_id'] = cidade_row['id']
        st.session_state['estado_cidade'] = cidade_row['state']
        st.session_state['selected_sep'] = None

        with st.spinner("Carregando anos disponíveis..."):
            anos_disponiveis = load_anos_para_cidade(st.session_state['cidade_id'])

        if anos_disponiveis:
            default_index = 0
            if " Presente" in anos_disponiveis:
                default_index = anos_disponiveis.index(" Presente")
            selected_ano = st.selectbox(
                label="Selecione o ano",
                options=anos_disponiveis,
                index=default_index,
                format_func=lambda x: x.strip(),
                label_visibility="collapsed",
                placeholder="Escolha um ano",
                key="ano_select"
            )
            st.session_state['selected_ano'] = selected_ano
        else:
            st.warning("Nenhum ano disponível para esta cidade.")
            st.session_state['selected_ano'] = None
    else:
        st.session_state['cidade_id'] = None
        st.session_state['estado_cidade'] = None
        st.session_state['selected_ano'] = None
        st.session_state['selected_sep'] = None

    if st.session_state['cidade_id'] and st.session_state['selected_ano']:
        with st.spinner("Carregando setores disponíveis..."):
            setores_disponiveis = load_setores_para_cidade_ano(
                st.session_state['cidade_id'],
                st.session_state['selected_ano']
            )

        opcoes_setores = ["Selecione o Setor Estratégico desejado"] + setores_disponiveis
        selected_sep = st.selectbox(
            label="Selecione o setor",
            options=opcoes_setores,
            index=0,
            label_visibility="collapsed",
            placeholder="Escolha um setor",
            key="sep_select"
        )
        st.session_state['selected_sep'] = selected_sep
    else:
        st.session_state['selected_sep'] = None

    if st.session_state['cidade_id'] and st.session_state['selected_ano']:
        with st.spinner("Carregando mapa da cidade..."):
            cidade_features, lat, lon = load_city_geojson(st.session_state['cidade_id'])

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
                map_style='https://basemaps.cartocdn.com/gl/positron-gl-style/style.json',
                tooltip={"text": "{name}"}
            )

            st.pydeck_chart(deck, width='stretch', height=400)
        else:
            st.warning("Geometria da cidade não disponível.")
    elif st.session_state['cidade_id']:
        st.info("Selecione um ano para visualizar o mapa e os dados.")

    if (st.session_state['cidade_id'] and
            st.session_state['selected_ano'] and
            st.session_state['selected_sep'] and
            st.session_state['selected_sep'] != "Selecione o Setor Estratégico desejado"):
        with st.spinner("Carregando ranking..."):
            df_rank = load_ranking_data(
                st.session_state['cidade_id'],
                st.session_state['selected_ano'],
                st.session_state['selected_sep']
            )
        if not df_rank.empty:
            st.markdown("### Ranking")
            df_display = df_rank[['label', 'resolucao', 'ranking', 'total_lines']].copy()
            df_display['ranking/total_lines'] = df_display['ranking'].astype(str) + "/" + df_display[
                'total_lines'].astype(str)
            rows_html = []
            for _, row in df_display.iterrows():
                rows_html.append("<tr style='border: 0;'>")
                rows_html.append(
                    "<td style='padding:8px 12px; border:none; font-family:sans-serif;'>" + row['label'] + "<" + "/td>")
                rows_html.append("<td style='padding:8px 12px; border:none; font-family:sans-serif;'>" + row[
                    'resolucao'] + "<" + "/td>")
                rows_html.append(
                    "<td style='padding:8px 12px; border:none; text-align:right; font-family:monospace; font-weight:bold;'>" +
                    row['ranking/total_lines'] + "<" + "/td>")
                rows_html.append("<" + "/tr>")
            html_table = "<div style='max-height:300px; overflow-y:auto;'><table style='border-collapse:collapse; width:100%;'>" + "".join(
                rows_html) + "<" + "/table></div>"
            st.markdown(html_table, unsafe_allow_html=True)
        else:
            st.info("Nenhum dado de ranking disponível para este setor.")

with col_direita:
    if st.session_state['cidade_id'] and st.session_state['selected_ano']:
        with st.spinner("Carregando indicadores..."):
            df_dados = load_county_data_view(
                st.session_state['cidade_id'],
                st.session_state['selected_ano'],
                st.session_state['selected_sep']
            )

        if not df_dados.empty:
            rows = []
            for _, row in df_dados.iterrows():
                rows.append("<tr style='border: 0;'>")
                rows.append(
                    "<td style='padding: 5px 15px 5px 5px; text-align: center; border: none;' class='tooltip-cell' data-tooltip='" +
                    row['sep'] + "'><img src='" + row['imageurl'] + "' width='32' height='32'><" + "/td>")
                rows.append(
                    "<td style='padding: 5px 5px 5px 10px; text-align: left; font-family: \"Courier New\", monospace; font-weight: bold; background-color: " +
                    row[
                        'color'] + "; border-radius: 4px; line-height: 32px; border: none;' class='tooltip-cell' data-tooltip='" +
                    row['sep'] + "'>" + f"{row['value']:.3f}" + "<" + "/td>")
                rows.append(
                    "<td style='padding: 5px 10px 5px 15px; text-align: left; font-family: sans-serif; border: none;'>" +
                    row['label'] + "<" + "/td>")
                rows.append("<" + "/tr>")
            html = "<div style='max-height: 450px; overflow-y: auto; font-family: sans-serif; margin-top: 20px; padding-bottom: 50px;'><table style='border-collapse: collapse; border: 0; margin-right: auto;'>" + "".join(
                rows) + "<" + "/table></div>"
            st.markdown(html, unsafe_allow_html=True)
        else:
            st.info("Nenhum dado disponível para este município no ano e setor selecionados.")
    else:
        st.info("Selecione um município e um ano para visualizar os indicadores.")

# Rodapé
st.markdown("---")
st.caption("Fonte: adaptabrasil")