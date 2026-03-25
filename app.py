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

# CSS personalizado com fundo temático de mudanças climáticas
st.markdown("""
<style>
    /* Fundo da aplicação com gradiente suave de céu a terra */
    .stApp {
        background: linear-gradient(145deg, #a8d8ea 0%, #f0f0c0 50%, #d2b48c 100%);
        background-attachment: fixed;
    }
    /* Overlay semi-transparente para melhor contraste do conteúdo */
    .main > .block-container {
        background-color: rgba(255, 255, 255, 0.85);
        border-radius: 20px;
        padding: 1rem 2rem !important;
        margin-top: 1rem;
        margin-bottom: 1rem;
        box-shadow: 0 4px 20px rgba(0,0,0,0.1);
        backdrop-filter: blur(3px);
    }
    /* Ajustes de padding e borda (mantidos) */
    [data-testid="collapsedControl"] {
        display: none;
    }
    .stApp > header {
        display: none;
    }
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
    # Função para gerar o PDF em memória
    def generate_pdf():
        buffer = BytesIO()
        c = canvas.Canvas(buffer, pagesize=letter)
        c.setFont("Helvetica", 24)
        c.drawString(100, 750, "Plano de Adaptação")
        c.setFont("Helvetica", 12)
        c.drawString(100, 700, "Documento gerado pelo Painel Municipal")
        c.save()
        buffer.seek(0)
        return buffer.getvalue()

    # Botão de download direto
    st.download_button(
        label="Plano de Adaptação",
        data=generate_pdf(),
        file_name="plano_adaptacao.pdf",
        mime="application/pdf",
        key="plan_download"
    )

# -------------------------------------------------------------------
# Funções de carregamento de dados com cache (sem spinner do sistema)
# -------------------------------------------------------------------
@st.cache_data(ttl=600, show_spinner=False)
def load_municipios():
    """
    Carrega a lista de municípios: id, state e nome formatado como "nome - UF"
    """
    query = """
            SELECT id, state, CONCAT(name, ' - ', state) AS display
            FROM adaptabrasil.county
            ORDER BY display; \
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
    """
    Retorna lista de anos (valores originais, podendo conter espaços)
    """
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
    """
    Carrega os dados da view para a cidade, ano e setor (opcional).
    Inclui os campos label e order.
    """
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
def load_pie_data(state, year, sep):
    """
    Retorna DataFrame com color, label, order e count de municípios distintos no estado,
    para o ano e setor especificados. Agrupado por color, label e order.
    """
    query = f"""
    SELECT color, label, MIN("order") as ord, COUNT(DISTINCT county_id) as count
    FROM adaptabrasil.mv_painel_municipal
    WHERE state = '{state}' AND "year" = '{year}' AND sep = '{sep}'
    GROUP BY color, label, "order"
    ORDER BY "order";
    """
    try:
        conn = get_connection()
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Erro ao carregar dados do gráfico: {e}")
        return pd.DataFrame()


# -------------------------------------------------------------------
# Carregar lista de municípios (inicial)
# -------------------------------------------------------------------
with st.spinner("Carregando municípios..."):
    df_municipios = load_municipios()

# Inicializar session_state se necessário
if 'cidade_id' not in st.session_state:
    st.session_state['cidade_id'] = None
    st.session_state['estado_cidade'] = None
    st.session_state['selected_ano'] = None
    st.session_state['selected_sep'] = None

# -------------------------------------------------------------------
# Layout em duas colunas
# -------------------------------------------------------------------
col_esquerda, col_direita = st.columns([1, 1], gap="large")

with col_esquerda:
    # Seleção da cidade
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

    # Se cidade selecionada, atualizar session_state, resetar setor e carregar anos
    if selected_display:
        cidade_row = df_municipios[df_municipios['display'] == selected_display].iloc[0]
        st.session_state['cidade_id'] = cidade_row['id']
        st.session_state['estado_cidade'] = cidade_row['state']
        # Resetar setor ao mudar de cidade
        st.session_state['selected_sep'] = None

        with st.spinner("Carregando anos disponíveis..."):
            anos_disponiveis = load_anos_para_cidade(st.session_state['cidade_id'])

        if anos_disponiveis:
            # Define índice padrão para " Presente" (se existir)
            default_index = 0
            if " Presente" in anos_disponiveis:
                default_index = anos_disponiveis.index(" Presente")
            # Usa format_func para remover espaços na exibição
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

    # Se cidade e ano selecionados, carregar setores
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

    # Mapa da cidade
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

with col_direita:
    if st.session_state['cidade_id'] and st.session_state['selected_ano']:
        # Tabela de indicadores
        with st.spinner("Carregando indicadores..."):
            df_dados = load_county_data_view(
                st.session_state['cidade_id'],
                st.session_state['selected_ano'],
                st.session_state['selected_sep']
            )

        if not df_dados.empty:
            # HTML correto para a tabela
            html = """
            <div style='max-height: 450px; overflow-y: auto; font-family: sans-serif; margin-top: 20px; padding-bottom: 50px;'>
                <table style='border-collapse: collapse; border: 0; margin-right: auto;'>
            """
            for _, row in df_dados.iterrows():
                html += "<tr style='border: 0;'>"
                # Ícone
                html += f"<td style='padding: 5px 15px 5px 5px; text-align: center; border: none;' class='tooltip-cell' data-tooltip='{row['sep']}'><img src='{row['imageurl']}' width='32' height='32'></td>"
                # Valor com cor de fundo
                html += f"<td style='padding: 5px 5px 5px 10px; text-align: left; font-family: \"Courier New\", monospace; font-weight: bold; background-color: {row['color']}; border-radius: 4px; line-height: 32px; border: none;' class='tooltip-cell' data-tooltip='{row['sep']}'>{row['value']:.3f}</td>"
                # Label
                html += f"<td style='padding: 5px 10px 5px 15px; text-align: left; font-family: sans-serif; border: none;'>{row['label']}</td>"
                html += "</tr>"
            html += """
                </table>
            </div>
            """
            st.markdown(html, unsafe_allow_html=True)

            # Gráfico de pizza (se um setor específico foi selecionado)
            if (st.session_state['selected_sep'] and
                    st.session_state['selected_sep'] != "Selecione o Setor Estratégico desejado"):
                with st.spinner("Carregando gráfico..."):
                    df_pie = load_pie_data(
                        st.session_state['estado_cidade'],
                        st.session_state['selected_ano'],
                        st.session_state['selected_sep']
                    )
                if not df_pie.empty:
                    # Mapear cada cor para ela mesma (para colorir as fatias)
                    color_map = {color: color for color in df_pie['color'].unique()}
                    fig = px.pie(
                        df_pie,
                        values='count',
                        names='label',
                        title=f"Posição Relativa do Município no Estado ({st.session_state['estado_cidade']})",
                        color='color',
                        color_discrete_map=color_map,
                        category_orders={"label": df_pie['label'].tolist()}
                    )
                    fig.update_traces(textposition='inside', textinfo='percent+label')
                    st.plotly_chart(fig, width='stretch')
                else:
                    st.info("Nenhum dado para o gráfico de pizza.")
        else:
            st.info("Nenhum dado disponível para este município no ano e setor selecionados.")
    else:
        st.info("Selecione um município e um ano para visualizar os indicadores.")

# Rodapé
st.markdown("---")
st.caption("Fonte: adaptabrasil")