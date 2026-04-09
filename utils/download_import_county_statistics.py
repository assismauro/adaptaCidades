import pandas as pd
import requests
import psycopg2
import warnings

warnings.filterwarnings('ignore')

# Configurações do banco
DB_CONFIG = {
    "host": "satelier.dev.br",
    "port": 45345,
    "database": "adaptabrasil",
    "user": "canoa_power",
    "password": "SAtelier30140-097"
}

# -------------------------------------------------------------------
# 1. POPULAÇÃO - IBGE (Estimativas 2024)
# -------------------------------------------------------------------
def download_populacao():
    """
    Baixa as estimativas populacionais dos municípios do IBGE (2024)
    """
    print("Baixando dados de população (IBGE 2024)...")

    url = "https://ftp.ibge.gov.br/Estimativas_de_Populacao/Estimativas_2024/estimativa_dou_2024.xls"

    try:
        # Tentar ler com header=0 e depois filtrar
        df_raw = pd.read_excel(url, sheet_name='Municípios', header=0)

        # Encontrar a linha onde começam os dados (procura por 'COD. MUNIC' ou números)
        start_row = 0
        for i, row in df_raw.iterrows():
            if i > 0:
                first_val = str(row.iloc[0])
                if first_val.isdigit() or (first_val and first_val.strip() and first_val[0].isdigit()):
                    start_row = i
                    break

        if start_row > 0:
            df_raw = df_raw.iloc[start_row:].reset_index(drop=True)
            df_raw.columns = df_raw.iloc[0]
            df_raw = df_raw[1:].reset_index(drop=True)

        # Definir nomes das colunas
        colunas_map = {}
        for col in df_raw.columns:
            col_str = str(col).strip().upper()
            if 'COD. MUNIC' in col_str or 'COD_MUNIC' in col_str:
                colunas_map['cod_municipio'] = col
            elif 'NOME DO MUNICÍPIO' in col_str or 'NOME MUNICIPIO' in col_str:
                colunas_map['municipio'] = col
            elif 'POPULAÇÃO ESTIMADA' in col_str:
                colunas_map['populacao'] = col
            elif 'UF' in col_str and len(col_str) <= 3:
                colunas_map['uf'] = col

        # Selecionar colunas
        df_pop = pd.DataFrame()
        df_pop['uf'] = df_raw[colunas_map.get('uf', df_raw.columns[0])]
        df_pop['cod_municipio'] = pd.to_numeric(df_raw[colunas_map.get('cod_municipio', df_raw.columns[2])],
                                                errors='coerce')
        df_pop['municipio'] = df_raw[colunas_map.get('municipio', df_raw.columns[3])]
        df_pop['populacao'] = pd.to_numeric(df_raw[colunas_map.get('populacao', df_raw.columns[4])], errors='coerce')

    except Exception as e:
        print(f"Erro ao ler Excel: {e}")
        # Fallback: usar arquivo local se disponível
        print("Tentando usar arquivo local...")
        df_raw = pd.read_excel('estimativa_dou_2024.xls', sheet_name='Municípios', header=6)
        df_raw = df_raw.iloc[:, :5]
        df_raw.columns = ['uf', 'cod_uf', 'cod_municipio', 'municipio', 'populacao']
        df_pop = df_raw.copy()

    # Limpar dados
    df_pop = df_pop[df_pop['cod_municipio'].notna()]
    df_pop = df_pop[df_pop['cod_municipio'] > 0]
    df_pop['cod_municipio'] = df_pop['cod_municipio'].astype(int)
    df_pop['populacao'] = df_pop['populacao'].fillna(0).astype(int)
    df_pop['ano'] = 2024

    print(f"  - {len(df_pop)} municípios carregados")
    return df_pop[['cod_municipio', 'municipio', 'uf', 'populacao', 'ano']]


# -------------------------------------------------------------------
# 2. PIB MUNICIPAL - IBGE (PIB per capita 2021)
# -------------------------------------------------------------------
def download_pib():
    """
    Baixa dados de PIB municipal do IBGE (2021)
    """
    print("Baixando dados de PIB (IBGE 2021)...")

    url = "https://sidra.ibge.gov.br/geratabela?format=CSV&name=tabela5938.csv&terr=NR&rank=-&query=t/5938/n1/all/v/37/p/2021"

    response = requests.get(url)
    content = response.text

    with open('temp_pib.csv', 'w', encoding='latin1') as f:
        f.write(content)

    df_pib = pd.read_csv('temp_pib.csv', sep=';', encoding='latin1', skiprows=3)

    df_pib['cod_municipio'] = df_pib['Município'].str.extract(r'(\d{7})')
    df_pib['pib_per_capita'] = pd.to_numeric(df_pib['Valor'].astype(str).str.replace(',', '.'), errors='coerce')

    df_pib = df_pib[df_pib['cod_municipio'].notna()]
    df_pib['cod_municipio'] = df_pib['cod_municipio'].astype(int)
    df_pib = df_pib.groupby('cod_municipio', as_index=False)['pib_per_capita'].mean()
    df_pib['ano'] = 2021

    print(f"  - {len(df_pib)} municípios carregados")
    return df_pib


# -------------------------------------------------------------------
# 3. IDH - Atlas Brasil (2010)
# -------------------------------------------------------------------
def download_idh():
    """
    Baixa dados de IDH municipal do Atlas Brasil (2010)
    """
    print("Baixando dados de IDH (Atlas Brasil 2010)...")

    url = "http://www.atlasbrasil.org.br/arquivos/municipios/2010/municipios_idhm_2010.xls"

    df_idh = pd.read_excel(url)
    df_idh = df_idh[['Codmun7', 'IDHM', 'IDHM_Renda', 'IDHM_Longevidade', 'IDHM_Educacao']]
    df_idh.columns = ['cod_municipio', 'idhm', 'idhm_renda', 'idhm_longevidade', 'idhm_educacao']
    df_idh['ano'] = 2010

    print(f"  - {len(df_idh)} municípios carregados")
    return df_idh


# -------------------------------------------------------------------
# 4. Importar para PostgreSQL
# -------------------------------------------------------------------
def importar_dados():
    """
    Baixa e importa todos os dados para o PostgreSQL
    """
    # Conectar ao banco
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Criar schema se não existir
    cursor.execute("CREATE SCHEMA IF NOT EXISTS painel_municipal;")

    # Criar tabelas
    cursor.execute("""
                   CREATE TABLE IF NOT EXISTS painel_municipal.populacao
                   (
                       cod_municipio
                       INTEGER
                       PRIMARY
                       KEY,
                       municipio
                       VARCHAR
                   (
                       100
                   ),
                       uf VARCHAR
                   (
                       2
                   ),
                       populacao INTEGER,
                       ano INTEGER
                       );
                   """)

    cursor.execute("""
                   CREATE TABLE IF NOT EXISTS painel_municipal.pib
                   (
                       cod_municipio
                       INTEGER
                       PRIMARY
                       KEY,
                       pib_per_capita
                       DECIMAL
                   (
                       15,
                       2
                   ),
                       ano INTEGER
                       );
                   """)

    cursor.execute("""
                   CREATE TABLE IF NOT EXISTS painel_municipal.idh
                   (
                       cod_municipio
                       INTEGER
                       PRIMARY
                       KEY,
                       idhm
                       DECIMAL
                   (
                       5,
                       4
                   ),
                       idhm_renda DECIMAL
                   (
                       5,
                       4
                   ),
                       idhm_longevidade DECIMAL
                   (
                       5,
                       4
                   ),
                       idhm_educacao DECIMAL
                   (
                       5,
                       4
                   ),
                       ano INTEGER
                       );
                   """)

    cursor.execute("""
                   CREATE TABLE IF NOT EXISTS painel_municipal.dados_municipios
                   (
                       cod_municipio
                       INTEGER
                       PRIMARY
                       KEY,
                       municipio
                       VARCHAR
                   (
                       100
                   ),
                       uf VARCHAR
                   (
                       2
                   ),
                       populacao INTEGER,
                       ano_populacao INTEGER,
                       pib_per_capita DECIMAL
                   (
                       15,
                       2
                   ),
                       ano_pib INTEGER,
                       idhm DECIMAL
                   (
                       5,
                       4
                   ),
                       idhm_renda DECIMAL
                   (
                       5,
                       4
                   ),
                       idhm_longevidade DECIMAL
                   (
                       5,
                       4
                   ),
                       idhm_educacao DECIMAL
                   (
                       5,
                       4
                   ),
                       ano_idh INTEGER
                       );
                   """)

    conn.commit()
    print("Tabelas criadas no schema painel_municipal")

    # Baixar dados
    df_pop = download_populacao()
    df_pib = download_pib()
    df_idh = download_idh()

    print("\nImportando dados...")

    # Importar população
    for _, row in df_pop.iterrows():
        cursor.execute("""
                       INSERT INTO painel_municipal.populacao (cod_municipio, municipio, uf, populacao, ano)
                       VALUES (%s, %s, %s, %s, %s) ON CONFLICT (cod_municipio) DO
                       UPDATE SET
                           municipio = EXCLUDED.municipio,
                           uf = EXCLUDED.uf,
                           populacao = EXCLUDED.populacao,
                           ano = EXCLUDED.ano;
                       """, (row['cod_municipio'], row['municipio'], row['uf'], row['populacao'], row['ano']))

    # Importar PIB
    for _, row in df_pib.iterrows():
        cursor.execute("""
                       INSERT INTO painel_municipal.pib (cod_municipio, pib_per_capita, ano)
                       VALUES (%s, %s, %s) ON CONFLICT (cod_municipio) DO
                       UPDATE SET
                           pib_per_capita = EXCLUDED.pib_per_capita,
                           ano = EXCLUDED.ano;
                       """, (row['cod_municipio'], row['pib_per_capita'], row['ano']))

    # Importar IDH
    for _, row in df_idh.iterrows():
        cursor.execute("""
                       INSERT INTO painel_municipal.idh (cod_municipio, idhm, idhm_renda, idhm_longevidade, idhm_educacao, ano)
                       VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (cod_municipio) DO
                       UPDATE SET
                           idhm = EXCLUDED.idhm,
                           idhm_renda = EXCLUDED.idhm_renda,
                           idhm_longevidade = EXCLUDED.idhm_longevidade,
                           idhm_educacao = EXCLUDED.idhm_educacao,
                           ano = EXCLUDED.ano;
                       """, (row['cod_municipio'], row['idhm'], row['idhm_renda'],
                             row['idhm_longevidade'], row['idhm_educacao'], row['ano']))

    conn.commit()

    # Criar tabela consolidada
    print("\nCriando tabela consolidada...")
    cursor.execute("TRUNCATE TABLE painel_municipal.dados_municipios;")

    cursor.execute("""
                   INSERT INTO painel_municipal.dados_municipios (cod_municipio, municipio, uf,
                                                                  populacao, ano_populacao,
                                                                  pib_per_capita, ano_pib,
                                                                  idhm, idhm_renda, idhm_longevidade, idhm_educacao,
                                                                  ano_idh)
                   SELECT p.cod_municipio,
                          p.municipio,
                          p.uf,
                          p.populacao,
                          p.ano,
                          pi.pib_per_capita,
                          pi.ano,
                          i.idhm,
                          i.idhm_renda,
                          i.idhm_longevidade,
                          i.idhm_educacao,
                          i.ano
                   FROM painel_municipal.populacao p
                            LEFT JOIN painel_municipal.pib pi ON p.cod_municipio = pi.cod_municipio
                            LEFT JOIN painel_municipal.idh i ON p.cod_municipio = i.cod_municipio ON CONFLICT (cod_municipio) DO
                   UPDATE SET
                       municipio = EXCLUDED.municipio,
                       uf = EXCLUDED.uf,
                       populacao = EXCLUDED.populacao,
                       ano_populacao = EXCLUDED.ano_populacao,
                       pib_per_capita = EXCLUDED.pib_per_capita,
                       ano_pib = EXCLUDED.ano_pib,
                       idhm = EXCLUDED.idhm,
                       idhm_renda = EXCLUDED.idhm_renda,
                       idhm_longevidade = EXCLUDED.idhm_longevidade,
                       idhm_educacao = EXCLUDED.idhm_educacao,
                       ano_idh = EXCLUDED.ano_idh;
                   """)

    conn.commit()

    cursor.close()
    conn.close()

    print(f"\n✅ Importação concluída!")
    print(f"   - População: {len(df_pop)} municípios")
    print(f"   - PIB: {len(df_pib)} municípios")
    print(f"   - IDH: {len(df_idh)} municípios")


# -------------------------------------------------------------------
# Executar
# -------------------------------------------------------------------
if __name__ == "__main__":
    importar_dados()