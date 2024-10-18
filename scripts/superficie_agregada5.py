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

# 2. Función para agregar superficies por barrio o distrito
def agregar_superficies(df):
    """
    Agregar superficies de terrazas por barrio o distrito.
    """
    if 'Superficie_ES' in df.columns and 'desc_barrio_local' in df.columns:
        # Agrupar por barrio y sumar las superficies
        superficies_agregadas = df.groupby('desc_barrio_local')['Superficie_ES'].sum().reset_index()
        
        # Renombrar la columna para mayor claridad
        superficies_agregadas.rename(columns={'Superficie_ES': 'Superficie_Total'}, inplace=True)

        # Documentar áreas con mayor y menor superficie agregada
        area_mayor = superficies_agregadas.loc[superficies_agregadas['Superficie_Total'].idxmax()]
        area_menor = superficies_agregadas.loc[superficies_agregadas['Superficie_Total'].idxmin()]

        logger.info(f"Área con mayor superficie: {area_mayor['desc_barrio_local']} - Superficie Total: {area_mayor['Superficie_Total']}")
        logger.info(f"Área con menor superficie: {area_menor['desc_barrio_local']} - Superficie Total: {area_menor['Superficie_Total']}")

        return superficies_agregadas
    else:
        logger.warning("Faltan columnas necesarias para la agregación.")
        return None

# 3. Procesamiento del dataset de terrazas para agregar superficies
def procesar_superficies():
    terrazas_path = RAW_DATA_PATHS['terrazas']  # Ajusta según tu configuración
    terrazas_df = load_csv_data(terrazas_path, 'Terrazas_202104.csv')

    if terrazas_df is not None:
        # Agregar superficies por barrio o distrito
        superficies_agregadas_df = agregar_superficies(terrazas_df)

        if superficies_agregadas_df is not None:
            # Crear el directorio si no existe
            output_dir = "data/cleaned_data"
            os.makedirs(output_dir, exist_ok=True)

            # Guardar el dataset de superficies agregadas
            archivo_salida_superficies = os.path.join(output_dir, "Superficies_Agregadas.csv")
            superficies_agregadas_df.to_csv(archivo_salida_superficies, index=False)
            logger.info(f"Datos de superficies agregadas guardados en '{archivo_salida_superficies}'")

if __name__ == "__main__":
    # Llamar a las funciones de procesamiento
    procesar_superficies()