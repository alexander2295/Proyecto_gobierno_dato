import os

# Configuraciones de conexión a MySQL
MYSQL_HOST = 'localhost'  # Cambia a la IP del servidor si no está en localhost
MYSQL_USER = 'root'
MYSQL_PASSWORD = 'barcelona_2295@'
MYSQL_DATABASE = 'proyecto_limpieza'  # Reemplaza con el nombre de tu base de datos

# Rutas de archivos
BASE_DIR = "C:/Users/Usuario/Desktop/Gobierno_Limpieza_data/"
RAW_DATA_PATHS = {
    "books": os.path.join(BASE_DIR, 'data/books.csv'),
    "licencias": os.path.join(BASE_DIR, 'data/Licencias_Locales_202104.csv'),
    "locales": os.path.join(BASE_DIR, 'data/Locales_202104.csv'),
    "terrazas": os.path.join(BASE_DIR, 'data/Terrazas_202104.csv')
}

CLEANED_DATA_PATH = os.path.join(BASE_DIR, 'data/cleaned_data/cleaned_data.parquet')

# Otras configuraciones
LOGGING_LEVEL = 'INFO'  # Nivel de logging
