import os
import pandas as pd
import logging
from config import RAW_DATA_PATHS, LOGGING_LEVEL  # Importar rutas y configuración

# Configuración de logging
logging.basicConfig(level=LOGGING_LEVEL)
logger = logging.getLogger(__name__)

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

# 2. Función para realizar el INNER JOIN entre los datasets
def realizar_join(terrazas_df, licencias_df):
    """
    Realizar un INNER JOIN entre los datasets de terrazas y licencias usando 'id_local'.
    """
    if 'id_local' in terrazas_df.columns and 'id_local' in licencias_df.columns:
        # Realizar el INNER JOIN
        merged_df = pd.merge(terrazas_df, licencias_df, on='id_local', how='inner')
        logger.info("INNER JOIN realizado exitosamente.")
        
        # Eliminar duplicados basados en la columna 'id_local'
        merged_df_sin_duplicados = merged_df.drop_duplicates(subset=['id_local'])
        logger.info(f"Eliminados {merged_df.shape[0] - merged_df_sin_duplicados.shape[0]} duplicados de 'id_local'.")
        
        return merged_df_sin_duplicados
    else:
        logger.warning("La columna 'id_local' no existe en uno o ambos DataFrames.")
        return None

# 3. Procesamiento del dataset
def procesar_datasets():
    # Definir la ruta de los archivos
    cleaned_data_path = r"C:\Users\Usuario\Desktop\Gobierno_Limpieza_data\scripts\data\cleaned_data"
    
    # Crear carpeta si no existe
    if not os.path.exists(cleaned_data_path):
        os.makedirs(cleaned_data_path)
        logger.info(f"Carpeta creada: {cleaned_data_path}")

    # Cargar los datasets
    terrazas_df = load_csv_data(os.path.join(cleaned_data_path, 'Terrazas_Normalizadas.csv'), 'Terrazas_Normalizadas.csv')
    licencias_df = load_csv_data(os.path.join(cleaned_data_path, 'Licencias_SinDuplicados.csv'), 'Licencias_SinDuplicados.csv')

    if terrazas_df is not None and licencias_df is not None:
        # Realizar el INNER JOIN
        merged_df = realizar_join(terrazas_df, licencias_df)

        if merged_df is not None:
            # Definir el archivo de salida
            archivo_salida_integrado = os.path.join(cleaned_data_path, "Licencias_Terrazas_Integradas.csv")

            try:
                # Guardar el dataset integrado sin duplicados
                merged_df.to_csv(archivo_salida_integrado, index=False)
                logger.info(f"Dataset integrado guardado en '{archivo_salida_integrado}'")
            except PermissionError as e:
                logger.error(f"Error de permisos al intentar guardar el archivo: {e}")
            except Exception as e:
                logger.error(f"Error inesperado al guardar el archivo: {e}")

if __name__ == "__main__":
    # Llamar a las funciones de procesamiento
    procesar_datasets()
