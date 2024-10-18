import pandas as pd
import pyodbc
import os

# Configuración de la conexión
server = 'DARY'
database = 'limpieza'  # Cambia por el nombre real
username = 'alex22'
password = 'Barcelona'
csv_directory = r'C:/Users/Usuario/Desktop/Gobierno_Limpieza_data/scripts/data/cleaned_data'

# Crear la conexión
try:
    conn = pyodbc.connect(
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={server};'
        f'DATABASE={database};'
        f'UID={username};'
        f'PWD={password}'
    )
    cursor = conn.cursor()
    
    # Recorrer todos los archivos CSV en el directorio
    for filename in os.listdir(csv_directory):
        if filename.endswith('.csv'):
            csv_file_path = os.path.join(csv_directory, filename)
            print(f'Procesando archivo: {filename}')

            # Leer el archivo CSV
            df = pd.read_csv(csv_file_path)

            # Verificar y convertir columnas a FLOAT
            float_columns = df.select_dtypes(include=['float64']).columns
            for col in float_columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')  # Convierte a float, establece nulos en inválidos

            # Manejar valores nulos
            df.fillna(0, inplace=True)  # Reemplaza valores nulos con 0

            # Crear la tabla si no existe
            create_table_query = f'CREATE TABLE {filename[:-4]} (id INT IDENTITY(1,1) PRIMARY KEY, '  # Nombre de la tabla sin .csv
            
            for column in df.columns:
                if df[column].dtype == 'object':
                    create_table_query += f"{column} VARCHAR(MAX), "
                elif df[column].dtype == 'int64':
                    create_table_query += f"{column} INT, "
                elif df[column].dtype == 'float64':
                    create_table_query += f"{column} FLOAT, "
                else:
                    create_table_query += f"{column} VARCHAR(255), "  # Tipo por defecto

            create_table_query = create_table_query.rstrip(', ') + ')'
            
            cursor.execute(f'''
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{filename[:-4]}' AND xtype='U')
            {create_table_query}
            ''')
            
            # Insertar los datos del DataFrame en la tabla
            for index, row in df.iterrows():
                cursor.execute(f'''
                INSERT INTO {filename[:-4]} ({', '.join(df.columns)}) 
                VALUES ({', '.join(['?' for _ in df.columns])})
                ''', tuple(row))

            conn.commit()  # Guardar los cambios
            print(f"Datos de {filename} insertados con éxito.")

except Exception as e:
    print("Error en la conexión o en la inserción:", e)

finally:
    if 'cursor' in locals():
        cursor.close()
    conn.close()