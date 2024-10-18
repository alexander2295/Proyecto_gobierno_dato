import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from config import RAW_DATA_PATHS, LOGGING_LEVEL

# Configurar logging
logging.basicConfig(level=LOGGING_LEVEL)
logger = logging.getLogger(__name__)

def load_data():
    """Función para cargar los archivos de datos en DataFrames de PySpark dinámicamente según su tipo."""
    # Crear sesión de Spark
    spark = SparkSession.builder \
        .appName("Carga de Datos") \
        .getOrCreate()
    
    data_frames = {}

    # Lista de codificaciones a probar
    codificaciones = ['utf-8', 'ISO-8859-1', 'latin1', 'cp1252', 'utf-16']
    
    # Iteramos sobre los archivos en RAW_DATA_PATHS
    for file_key, file_path in RAW_DATA_PATHS.items():
        try:
            # Verificamos la extensión del archivo para determinar cómo cargarlo
            file_extension = os.path.splitext(file_path)[1].lower()

            if file_extension == '.json':
                logger.info(f"Cargando archivo {file_key} desde {file_path} (JSON)...")
                df = spark.read.json(file_path)

            elif file_extension == '.csv':
                logger.info(f"Cargando archivo {file_key} desde {file_path} (CSV)...")
                
                # Intentar cargar el archivo CSV con diferentes codificaciones
                for codificacion in codificaciones:
                    try:
                        # Cargar el archivo CSV con la codificación especificada
                        df = spark.read.option("header", "true").option("charset", codificacion).csv(file_path)
                        logger.info(f"El archivo se cargó correctamente con la codificación '{codificacion}'.")
                        break  # Salir del bucle si se carga correctamente
                    except AnalysisException:
                        logger.warning(f"No se pudo cargar el archivo con la codificación '{codificacion}'. Intentando con la siguiente...")
                        continue  # Continuar con la siguiente codificación
            
            else:
                logger.warning(f"Formato no reconocido para el archivo {file_key}: {file_extension}")
                continue  # Si el formato no es reconocido, saltamos este archivo

            # Guardamos el DataFrame cargado en el diccionario
            data_frames[file_key] = df
            logger.info(f"{file_key} cargado exitosamente con {df.count()} filas y {len(df.columns)} columnas.")
        
        except Exception as e:
            logger.error(f"Error al cargar el archivo {file_key}: {e}")
            continue  # Continuar con el siguiente archivo en lugar de salir

    return data_frames

if __name__ == "__main__":
    logger.info("Iniciando el proceso de extracción de datos...")
    data_frames = load_data()
    
    # Confirmamos cuántos archivos fueron cargados
    logger.info(f"Extracción de datos completada. Se han cargado {len(data_frames)} archivos.")
