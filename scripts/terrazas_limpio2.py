import os
from pyspark.sql import SparkSession
import pandas as pd
import numpy as np
import logging
from config import RAW_DATA_PATHS, LOGGING_LEVEL  # Importar rutas y configuración

# Configuración de logging
logging.basicConfig(level=LOGGING_LEVEL)
logger = logging.getLogger(__name__)

# Iniciar una sesión de Spark
spark = SparkSession.builder.appName("CargaYLimpezaDatos").getOrCreate()

# 1. Función para cargar un archivo CSV en un DataFrame de Pandas con manejo de codificación
def load_csv_data(file_path, file_name):
    """
    Cargar un archivo CSV en un DataFrame de Pandas, manejando diferentes codificaciones.
    """
    encodings = ['utf-8', 'ISO-8859-1']  # Lista de codificaciones a probar
    
    for encoding in encodings:
        try:
            logger.info(f"Cargando archivo {file_name} con codificación '{encoding}'...")
            df = pd.read_csv(file_path, encoding=encoding)
            logger.info(f"Archivo {file_name} cargado exitosamente con {df.shape[0]} filas y {df.shape[1]} columnas.")
            return df
        except Exception as e:
            logger.warning(f"Error al cargar el archivo con codificación '{encoding}': {e}")
    
    logger.error(f"No se pudo cargar el archivo {file_name} con ninguna de las codificaciones.")
    return None

# 2. Función para filtrar registros con más del 50% de valores nulos
def filtrar_registros_nulos(df):
    """
    Eliminar registros con más del 50% de valores nulos.
    """
    threshold = len(df.columns) * 0.5  # 50% del total de columnas
    initial_count = len(df)  # Número de registros originales
    df_filtered = df.dropna(thresh=threshold)

    # Rellenar NaN en columnas numéricas con un valor específico (por ejemplo, 0)
    df_filtered.fillna(0, inplace=True)

    final_count = len(df_filtered)  # Número de registros después del filtro
    logger.info(f"Registros eliminados: {initial_count - final_count}")
    logger.info(f"Registros restantes: {final_count}")
    
    return df_filtered

# 3. Función para normalizar columnas numéricas (sin logaritmo para Superficie_ES)
def normalizar_columnas(df):
    """
    Normalizar las columnas numéricas seleccionadas.
    """
    # Convertir 'Superficie_ES' a flotante sin aplicar logaritmo
    if 'Superficie_ES' in df.columns:
        df['Superficie_ES'] = pd.to_numeric(df['Superficie_ES'], errors='coerce')
        logger.info("Columna 'Superficie_ES' convertida a flotante.")
    
    return df

# 4. Función para calcular el ratio entre 'Superficie_ES' y 'id_terraza'
def calcular_ratio(df):
    """
    Calcular el ratio entre 'Superficie_ES' y 'id_terraza'.
    """
    if 'Superficie_ES' in df.columns and 'id_terraza' in df.columns:
        # Asegurarse que ambas columnas son numéricas
        df['Superficie_ES'] = pd.to_numeric(df['Superficie_ES'], errors='coerce')
        df['id_terraza'] = pd.to_numeric(df['id_terraza'], errors='coerce')

        # Calcular el ratio y agregarlo como una nueva columna
        df['ratio_Superficie_id'] = df['Superficie_ES'] / df['id_terraza'].replace(0, np.nan)  # Evitar división por cero
        
        logger.info("Ratio calculado exitosamente entre 'Superficie_ES' y 'id_terraza'.")
        
        return df
    else:
        logger.warning("Una o ambas columnas necesarias para calcular el ratio no existen.")
        return df

# 5. Función para convertir horas a formato adecuado (ejemplo)
def convertir_horas(df):
    """
    Convertir columnas de horas en un formato adecuado.
    """
    if 'horas' in df.columns:
        df['horas'] = pd.to_datetime(df['horas'], format='%H:%M', errors='coerce')
        logger.info("Columna 'horas' convertida a formato de tiempo.")
    
    return df

# 6. Procesamiento del dataset de terrazas
def procesar_terrazas():
    terrazas_path = RAW_DATA_PATHS['terrazas']  # Ajusta según tu configuración
    terrazas_df = load_csv_data(terrazas_path, 'Terrazas_202104.csv')

    if terrazas_df is not None:
        # Eliminar las columnas innecesarias: 'Escalera', 'id_local_agrupado', 'id_clase_ndp_edificio', 'sillas_ra', y 'Superficie_TO'
        terrazas_df.drop(columns=['Escalera', 'id_local_agrupado', 'id_clase_ndp_edificio', 'sillas_ra', 'Superficie_TO'], inplace=True, errors='ignore')

        # Filtrado de registros con nulos
        terrazas_df_filtrado = filtrar_registros_nulos(terrazas_df)

        # Convertir horas a formato adecuado (si es necesario)
        terrazas_df_convertido = convertir_horas(terrazas_df_filtrado)

        # Normalización de columnas numéricas (sin logaritmo)
        terrazas_df_normalizado = normalizar_columnas(terrazas_df_convertido)

        # Calcular el ratio entre 'Superficie_ES' y 'id_terraza'
        terrazas_df_con_ratio = calcular_ratio(terrazas_df_normalizado)

        # Crear el directorio si no existe
        output_dir = "data/cleaned_data"
        os.makedirs(output_dir, exist_ok=True)

        # Guardar el dataset normalizado con el nuevo ratio calculado
        archivo_salida_terrazas = os.path.join(output_dir, "Terrazas_Normalizadas.csv")
        terrazas_df_con_ratio.to_csv(archivo_salida_terrazas, index=False)
        logger.info(f"Datos normalizados de terrazas guardados en '{archivo_salida_terrazas}'")

if __name__ == "__main__":
    # Llamar a las funciones de procesamiento
    procesar_terrazas()