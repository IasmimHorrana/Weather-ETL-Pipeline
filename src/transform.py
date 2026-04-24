import pandas as pd 
import numpy as np
import json
from pathlib import Path 


import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Caminho absoluto calculado de forma segura, independente de onde o script for rodado
DEFAULT_DATA_PATH = Path(__file__).parent.parent / "data" / "weather_data.json"

# 1. Função de Extração
def load_raw_json(path_name: str | Path) -> dict:
    """Apenas abre o arquivo e traz os dados para a memória como dicionário."""
    logging.info(f"Lendo JSON do caminho: {path_name}")
    
    path = Path(path_name) # Transforma a string em um objeto de caminho
    
    if not path.exists():
        logging.error(f"Arquivo não encontrado: {path}")
        raise FileNotFoundError(f"Path {path} does not exist")
    
    with open(path, 'r') as f:
        data = json.load(f)
    
    logging.info("JSON carregado na memória com sucesso.")
    return data # Retorna o dicionário, ainda não virou tabela!

# 2. Função de Flattening 
def flatten_to_dataframe(raw_data: dict) -> pd.DataFrame:
    """Recebe o dicionário bruto e usa o json_normalize para construir o DataFrame."""
    logging.info("Iniciando o achatamento (flattening) dos dados JSON.")
    
    df = pd.json_normalize(
        raw_data,
        record_path='weather',          
        record_prefix='weather.',       
        meta=[
            'base', 'visibility', 'dt', 'timezone', 'id', 'name', 'cod',
            ['coord', 'lon'], ['coord', 'lat'],
            ['main', 'temp'], ['main', 'feels_like'], ['main', 'temp_min'],
            ['main', 'temp_max'], ['main', 'pressure'], ['main', 'humidity'],
            ['wind', 'speed'], ['wind', 'deg'], ['wind', 'gust'],
            ['clouds', 'all'],
            ['sys', 'country'], ['sys', 'sunrise'], ['sys', 'sunset']
        ],
        errors='ignore'   
    )
    
    # O JSON de chuva fica de fora do record_path principal, extraímos manualmente:
    if 'rain' in raw_data and '1h' in raw_data['rain']:
        df['rain.1h'] = raw_data['rain']['1h']
    
    logging.info("Achatamento concluído. Tabela criada.")
    return df

# 3. Função de Tratamento de Datas e Fuso
def convert_timestamps_to_local(df: pd.DataFrame, timezone: str = 'America/Bahia') -> pd.DataFrame:
    """Converte Unix Timestamps para horários locais, cuidando do fuso."""
    logging.info(f"Convertendo timestamps para o fuso horário: {timezone}")
    
    # Criamos uma cópia para evitar warnings do Pandas (SettingWithCopyWarning)
    df = df.copy()
    
    time_columns = ['dt', 'sys.sunrise', 'sys.sunset']
    
    # Programação defensiva: só converte se a coluna de fato existir na tabela
    cols_to_convert = [col for col in time_columns if col in df.columns]
    
    for col in cols_to_convert:
        df[col] = (
            pd.to_datetime(df[col], unit='s')
              .dt.tz_localize('UTC')
              .dt.tz_convert(timezone)
        )
        
    logging.info("Conversão de datas finalizada com sucesso.")
    return df

# 4. Função de Validação de Schema (Tratamento de Chuva)
def validate_rain_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Garante que a coluna de chuva exista e preenche valores ausentes com 0.0."""
    logging.info("Validando schema para a coluna de chuva (rain.1h).")
    
    df = df.copy()
    
    if 'rain.1h' not in df.columns:
        logging.info("Coluna 'rain.1h' ausente (não choveu). Criando coluna zerada.")
        df['rain.1h'] = 0.0
    else:
        logging.info("Coluna 'rain.1h' encontrada. Preenchendo valores nulos com 0.0.")
        df['rain.1h'] = df['rain.1h'].fillna(0.0)
        
    logging.info("Validação de chuva finalizada com sucesso.")
    return df

# 5. Função de Limpeza e Padronização (Camada Silver)
def standardize_to_silver(df: pd.DataFrame) -> pd.DataFrame:
    """Remove colunas desnecessárias e renomeia para o padrão de banco de dados (Silver)."""
    logging.info("Iniciando limpeza e padronização (Camada Silver).")
    
    df = df.copy()
    
    # Colunas que não vão para o banco Silver
    colunas_drop = ['base', 'id', 'cod', 'sys.id', 'sys.type', 'weather.icon', 'weather.id']
    
    # Drop seguro/defensivo
    cols_to_drop = [c for c in colunas_drop if c in df.columns]
    if cols_to_drop:
        df = df.drop(columns=cols_to_drop)
        logging.info(f"Colunas removidas (Drop): {cols_to_drop}")
        
    # Mapeamento para o padrão (Tradução e Unidades Métrica)
    rename_map = {
        'name':           'cidade',
        'sys.country':    'pais',
        'coord.lat':      'latitude',
        'coord.lon':      'longitude',
        'dt':             'data_hora',
        'main.temp':      'temperatura_c',
        'main.feels_like':'sensacao_termica_c',
        'main.temp_min':  'temp_min_c',
        'main.temp_max':  'temp_max_c',
        'main.pressure':  'pressao_hpa',
        'main.humidity':  'umidade_pct',
        'wind.speed':     'vento_velocidade_ms',
        'wind.deg':       'vento_direcao_grau',
        'wind.gust':      'vento_rajada_ms',
        'rain.1h':        'chuva_1h_mm',
        'clouds.all':     'nuvens_pct',
        'visibility':     'visibilidade_m',
        'weather.main':        'condicao_clima',       
        'weather.description': 'descricao_clima',
        'sys.sunrise':    'nascer_sol',
        'sys.sunset':     'por_sol',
    }
    
    df = df.rename(columns=rename_map)
    logging.info("Colunas renomeadas e padronizadas com sucesso.")
    
    return df

# 6. Função de Enriquecimento de Dados (Regra de Negócio)
def calculate_risk_level(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula o nível de risco com base em condições climáticas usando vetorização."""
    logging.info("Calculando a coluna de Nível de Risco.")
    
    df = df.copy()
    
    chuva  = df['chuva_1h_mm']
    vento  = df['vento_velocidade_ms']
    umidade = df['umidade_pct']

    condicoes = [
        (chuva >= 50) | (vento >= 15),           # CRÍTICO
        (chuva >= 25),                           # ALERTA
        (chuva >= 5) & (umidade >= 80),          # ATENÇÃO
    ]
    valores = ['CRÍTICO', 'ALERTA', 'ATENÇÃO']

    df['nivel_risco'] = np.select(condicoes, valores, default='NORMAL')
    
    logging.info("Nível de risco calculado com sucesso.")
    return df

# 7. Função de Exportação (Load / Camada Silver)
def save_silver_data(df: pd.DataFrame, output_path: str | Path) -> None:
    """Salva o DataFrame processado em formato JSON para a camada Silver."""
    logging.info(f"Iniciando a exportação dos dados para: {output_path}")
    
    path = Path(output_path)
    
    # Garante que a pasta de destino exista
    path.parent.mkdir(parents=True, exist_ok=True)
    
    # Exportação nos padrões exigidos
    df.to_json(path, orient='records', indent=4, date_format='iso')
    
    logging.info(f"Dados exportados com sucesso! Shape final: {df.shape}")
    print("\n[✔] Pipeline Concluído!")
    print(f"Shape salvo: {df.shape}")
    print("\nTipos de Dados:")
    print(df.dtypes)

# --- ORQUESTRADOR DA ESTEIRA ---
def run_pipeline(input_path: str | Path, output_path: str | Path) -> pd.DataFrame:
    """Orquestra todas as máquinas da esteira de transformação."""
    logging.info("--- INICIANDO PIPELINE DE DADOS ---")
    
    # 1. (Extract)
    dicionario_cru = load_raw_json(input_path)
    
    # 2. (Transform)
    df_achatado    = flatten_to_dataframe(dicionario_cru)
    df_com_datas   = convert_timestamps_to_local(df_achatado)
    df_com_chuva   = validate_rain_schema(df_com_datas)
    df_silver      = standardize_to_silver(df_com_chuva)
    df_enriquecido = calculate_risk_level(df_silver)
    
    # 3. (Load)
    save_silver_data(df_enriquecido, output_path)
    
    return df_enriquecido

# --- ÁREA DE EXECUÇÃO ---
if __name__ == '__main__':
    # Definimos os caminhos absolutos seguros
    arquivo_entrada = DEFAULT_DATA_PATH
    arquivo_saida = Path(__file__).parent.parent / "data" / "weather_silver.json"
    
    df_final = run_pipeline(arquivo_entrada, arquivo_saida)
