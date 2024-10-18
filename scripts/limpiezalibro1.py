import os
from pyspark.sql import SparkSession
import pandas as pd
import numpy as np
import logging
import pymysql  # Asegúrate de tener esta biblioteca instalada
from sqlalchemy import create_engine  # Para facilitar la carga de DataFrames a MySQL
from config import RAW_DATA_PATHS, LOGGING_LEVEL  # Importar rutas y configuración
from datetime import datetime  # Importar para la validación de fechas

# Configuración de logging
logging.basicConfig(level=LOGGING_LEVEL)
logger = logging.getLogger(__name__)

# Iniciar una sesión de Spark
spark = SparkSession.builder.appName("CargaYLimpezaDatos").getOrCreate()

# 1. Función para cargar un archivo CSV en un DataFrame de Spark
def load_csv_data(file_path, file_name):
    """
    Cargar un archivo CSV en un DataFrame de Spark.
    """
    try:
        logger.info(f"Cargando archivo {file_name} desde {file_path}...")
        df = spark.read.csv(file_path, header=True, inferSchema=True)
        logger.info(f"Archivo {file_name} cargado exitosamente con {df.count()} filas y {len(df.columns)} columnas.")
        return df
    except Exception as e:
        logger.error(f"Error al cargar el archivo {file_name}: {e}")
        return None

# 2. Función para filtrar registros con más del 50% de valores nulos (en Pandas)
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

# 3. Función para normalizar columnas numéricas (sin logaritmo para pageCount)
def normalizar_columnas(df):
    """
    Normalizar las columnas numéricas seleccionadas.
    """
    # Convertir 'pageCount' a flotante sin aplicar logaritmo
    if 'pageCount' in df.columns:
        df.loc[:, 'pageCount'] = pd.to_numeric(df['pageCount'], errors='coerce')
        logger.info("Columna 'pageCount' convertida a flotante.")
    
    return df

# 4. Función para limpiar y normalizar cadenas de texto en columnas categóricas
def limpiar_cadenas_texto(df):
    """
    Aplicar limpieza y normalización a las columnas categóricas.
    """
    # Lista de columnas categóricas que deseas limpiar (ajusta según tus datos)
    columnas_categoricas = ['titulo', 'authors', 'categories', 'publishedDate']  # Añadir 'publishedDate'

    for col in columnas_categoricas:
        if col in df.columns:
            # Convertir a minúsculas y eliminar espacios innecesarios
            df[col] = df[col].str.lower().str.strip()

            # Eliminar instancias repetidas de 'PUBLISH' y otros errores comunes
            df[col] = df[col].str.replace(r'\bPUBLISH\b', '', regex=True)
            df[col] = df[col].str.replace(r'\bMEAP\b', '', regex=True)

            # Limpiar texto de caracteres no deseados (ejemplo: eliminar saltos de línea)
            df[col] = df[col].str.replace(r'\s+', ' ', regex=True).str.strip()

            # Validar y reemplazar valores nulos, vacíos o ceros por 'desconocido'
            df[col] = df[col].replace('', 'desconocido').replace(0, 'desconocido')
            df[col].fillna('desconocido', inplace=True)

            logger.info(f"Columna '{col}' limpiada y normalizada.")
        else:
            logger.warning(f"La columna '{col}' no existe en el DataFrame.")

    # Validación de la columna 'status'
    if 'status' in df.columns:
        df['status'] = df['status'].apply(lambda x: x if x in ['MEAP', 'PUBLISH'] else 'desconocido')
        logger.info("Columna 'status' validada y valores desconocidos reemplazados.")
    else:
        logger.warning("La columna 'status' no existe en el DataFrame.")

    # Validación de la columna 'publishedDate'
    if 'publishedDate' in df.columns:
        def validar_fecha(fecha):
            # Intentar convertir la fecha a datetime
            if fecha == 'Unknown':
                return 'desconocido'
            try:
                # Aquí se puede ajustar el formato de fecha si es necesario
                datetime.fromisoformat(fecha.replace('Z', '+00:00'))  # Convertir a un objeto datetime
                return fecha  # La fecha es válida
            except ValueError:
                return 'desconocido'  # Fecha no válida

        df['publishedDate'] = df['publishedDate'].apply(validar_fecha)
        logger.info("Columna 'publishedDate' validada y valores desconocidos reemplazados.")
    else:
        logger.warning("La columna 'publishedDate' no existe en el DataFrame.")

    return df

# 7. Procesamiento del dataset de libros
def procesar_libros():
    libros_path = RAW_DATA_PATHS['books']
    libros_df_spark = load_csv_data(libros_path, 'books')

    if libros_df_spark:
        # Convertir DataFrame de Spark a Pandas para la limpieza
        libros_df_pandas = libros_df_spark.toPandas()

        # Filtrado de registros con nulos
        libros_df_filtrado = filtrar_registros_nulos(libros_df_pandas)

        # Eliminar la columna '_id' si es necesario
        libros_df_filtrado.drop(columns=['_id'], inplace=True, errors='ignore')  # Ignorar si no existe

        # Normalización de columnas numéricas (sin logaritmo para pageCount)
        libros_df_normalizado = normalizar_columnas(libros_df_filtrado)

        # Limpiar y normalizar cadenas de texto en columnas categóricas
        libros_df_limpio = limpiar_cadenas_texto(libros_df_normalizado)

        # Crear el directorio si no existe
        output_dir = "data/cleaned_data"
        os.makedirs(output_dir, exist_ok=True)

        # Guardar el dataset limpio y normalizado
        archivo_salida_libros = os.path.join(output_dir, "libros_cleaned.csv")
        libros_df_limpio.to_csv(archivo_salida_libros, index=False)

if __name__ == "__main__":
    procesar_libros()
