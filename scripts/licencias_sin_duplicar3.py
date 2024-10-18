import os
import pandas as pd
import logging
from config import RAW_DATA_PATHS, LOGGING_LEVEL  # Ajusta según tu configuración

# Configuración de logging
logging.basicConfig(level=LOGGING_LEVEL)
logger = logging.getLogger(__name__)

# Función para cargar un archivo CSV en un DataFrame de Pandas
def load_csv_data(file_path, encoding='utf-8'):
    """
    Cargar un archivo CSV en un DataFrame de Pandas.
    :param encoding: Codificación del archivo CSV (por defecto es 'utf-8').
    """
    try:
        logger.info(f"Cargando archivo desde {file_path}...")
        df = pd.read_csv(file_path, encoding=encoding)  # Especifica la codificación aquí
        logger.info(f"Archivo cargado exitosamente con {df.shape[0]} filas y {df.shape[1]} columnas.")
        return df
    except Exception as e:
        logger.error(f"Error al cargar el archivo: {e}")
        return None

# 1. Función para filtrar registros con más del 50% de valores nulos
def filtrar_registros_nulos(df):
    """
    Eliminar registros con más del 50% de valores nulos.
    """
    threshold = len(df.columns) * 0.5  # 50% del total de columnas
    initial_count = df.shape[0]  # Número de registros originales
    df_filtered = df.dropna(thresh=threshold)

    # Documentar cuántos registros se eliminaron
    final_count = df_filtered.shape[0]  # Número de registros después del filtro
    records_removed = initial_count - final_count
    logger.info(f"Registros eliminados por más del 50% de nulos: {records_removed}")
    logger.info(f"Registros restantes: {final_count}")
    
    # Justificación: Se eliminan registros con más del 50% de nulos porque pueden afectar la calidad del análisis y dar resultados engañosos.
    if records_removed > 0:
        logger.info("Se eliminaron registros con un alto porcentaje de datos faltantes para asegurar la integridad del análisis.")
    
    return df_filtered

# 2. Función para eliminar filas duplicadas
def eliminar_duplicados(df):
    """
    Eliminar filas duplicadas basadas en columnas clave.
    """
    initial_count = df.shape[0]  # Número de registros originales
    df_cleaned = df.drop_duplicates(subset=['id_local', 'ref_licencia'], keep='first')
    
    # Documentar cuántos registros se eliminaron
    final_count = df_cleaned.shape[0]  # Número de registros después de eliminar duplicados
    duplicates_removed = initial_count - final_count
    logger.info(f"Registros eliminados por duplicados: {duplicates_removed}")
    
    return df_cleaned

# 3. Función para corregir valores en las columnas especificadas
def corregir_columnas(df):
    """
    Corregir los valores en las columnas especificadas y convertir fechas.
    """
    # Reemplazar caracteres extraños y corregir valores en 'desc_tipo_situacion_licencia'
    replacements_situacion = {
        'tramitando transmisiÃƒÂ³n de licencia': 'tramitando transmisión de licencia',
        'en tramitaciÃƒÂ³n': 'en tramitación',
        'concedida': 'concedida',
        'transmisiÃƒÂ³n de licencia concedida': 'transmisión de licencia concedida',
        'denegada': 'denegada'
    }
    
    df['desc_tipo_situacion_licencia'] = df['desc_tipo_situacion_licencia'].replace(replacements_situacion)

    # Reemplazar caracteres extraños y corregir valores en 'desc_tipo_licencia'
    replacements_licencia = {
        'transmisiÃƒÂ³n de licencia urbanÃƒÂ­stica': 'transmisión de licencia urbanística',
        'declaraciÃƒÂ³n responsable': 'declaración responsable',
        'licencia urbanÃƒÂ­stica': 'licencia urbanística'
    }
    
    df['desc_tipo_licencia'] = df['desc_tipo_licencia'].replace(replacements_licencia)

    # Convertir 'Fecha_Dec_Lic' a tipo fecha
    df['Fecha_Dec_Lic'] = pd.to_datetime(df['Fecha_Dec_Lic'], errors='coerce')
    df['Fecha_Dec_Lic'].fillna(pd.Timestamp('1900-01-01'), inplace=True)  # Reemplazar nulos por fecha por defecto

    # Eliminar columnas no deseadas
    columnas_a_eliminar = ['id_agrupacion', 'id_tipo_agrup', 'id_local_agrupado']
    df = df.drop(columns=columnas_a_eliminar, errors='ignore')  # Ignorar si no existen

    return df

# 4. Procesamiento del dataset "Licencias202104"
def procesar_licencias():
    licencias_path = RAW_DATA_PATHS['locales']
    # Especifica la codificación aquí, si es necesario
    licencias_df = load_csv_data(licencias_path, encoding='ISO-8859-1')

    if licencias_df is not None:
        # Filtrar registros con nulos
        licencias_df_filtrado = filtrar_registros_nulos(licencias_df)

        # Corregir columnas
        licencias_df_corregido = corregir_columnas(licencias_df_filtrado)

        # Eliminar duplicados
        licencias_df_sin_duplicados = eliminar_duplicados(licencias_df_corregido)

        # Crear el directorio si no existe
        output_dir = r"C:\Users\Usuario\Desktop\Gobierno_Limpieza_data\scripts\data\cleaned_data"  # Ruta actualizada
        os.makedirs(output_dir, exist_ok=True)

        # Guardar el dataset limpio sin duplicados
        archivo_salida_licencias = os.path.join(output_dir, "Licencias_SinDuplicados.csv")
        licencias_df_sin_duplicados.to_csv(archivo_salida_licencias, index=False)
        logger.info(f"Dataset limpio guardado como {archivo_salida_licencias}")

if __name__ == "__main__":
    procesar_licencias()
