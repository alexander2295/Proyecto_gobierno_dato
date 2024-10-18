@echo off
echo Iniciando el proceso de limpieza de datos...

rem Ejecutar el script de extracción de datos
echo Ejecutando extract_data.py...
python C:\Users\Usuario\Desktop\Gobierno_Limpieza_data\scripts\extract_data.py >> C:\Users\Usuario\Desktop\Gobierno_Limpieza_data\logs\extract_data.log 2>&1
if ERRORLEVEL 1 (
    echo Error en extract_data.py. Verifique el log.
    exit /b 1
)
rem Ejecutar el script de limpieza de libros
echo Ejecutando limpiezalibro1.py...
python C:\Users\Usuario\Desktop\Gobierno_Limpieza_data\scripts\limpiezalibro1.py >> C:\Users\Usuario\Desktop\Gobierno_Limpieza_data\logs\limpiezalibro1.log 2>&1
if ERRORLEVEL 1 (
    echo Error en limpiezalibro1.py. Verifique el log.
    exit /b 1
)


rem Ejecutar el script de limpieza de terrazas
echo Ejecutando terrazas_limpio2.py...
python C:\Users\Usuario\Desktop\Gobierno_Limpieza_data\scripts\terrazas_limpio2.py >> C:\Users\Usuario\Desktop\Gobierno_Limpieza_data\logs\terrazas_limpio2.log 2>&1
if ERRORLEVEL 1 (
    echo Error en terrazas_limpio2.py. Verifique el log.
    exit /b 1
)

rem Ejecutar el script de limpieza de licencias sin duplicar
echo Ejecutando licencias_sin_duplicar3.py...
python C:\Users\Usuario\Desktop\Gobierno_Limpieza_data\scripts\licencias_sin_duplicar3.py >> C:\Users\Usuario\Desktop\Gobierno_Limpieza_data\logs\licencias_sin_duplicar3.log 2>&1
if ERRORLEVEL 1 (
    echo Error en licencias_sin_duplicar3.py. Verifique el log.
    exit /b 1
)
rem Ejecutar el script de integración de archivos
echo Ejecutando archivos_integrados4.py...
python C:\Users\Usuario\Desktop\Gobierno_Limpieza_data\scripts\archivos_integrados4.py >> C:\Users\Usuario\Desktop\Gobierno_Limpieza_data\logs\archivos_integrados4.log 2>&1
if ERRORLEVEL 1 (
    echo Error en archivos_integrados4.py. Verifique el log.
    exit /b 1
)
rem Ejecutar el script de agregación de superficie
echo Ejecutando superficie_agregada5.py...
python C:\Users\Usuario\Desktop\Gobierno_Limpieza_data\scripts\superficie_agregada5.py >> C:\Users\Usuario\Desktop\Gobierno_Limpieza_data\logs\superficie_agregada5.log 2>&1
if ERRORLEVEL 1 (
    echo Error en superficie_agregada5.py. Verifique el log.
    exit /b 1
)


rem Ejecutar el script de concatenación de archivos
echo Ejecutando archivos_concatenado.py...
python C:\Users\Usuario\Desktop\Gobierno_Limpieza_data\scripts\archivos_concatenado.py >> C:\Users\Usuario\Desktop\Gobierno_Limpieza_data\logs\archivos_concatenado.log 2>&1
if ERRORLEVEL 1 (
    echo Error en archivos_concatenado.py. Verifique el log.
    exit /b 1
)

echo Limpieza completada. Enviando datos a SQL Server...

rem Ejecutar el script que carga los datos en SQL Server
echo Ejecutando sql_carga_d.py...
python C:\Users\Usuario\Desktop\Gobierno_Limpieza_data\scripts\sql_carga_d.py >> C:\Users\Usuario\Desktop\Gobierno_Limpieza_data\logs\sql_carga_d.log 2>&1
if ERRORLEVEL 1 (
    echo Error en sql_carga_d.py. Verifique el log.
    exit /b 1
)

echo Proceso completado.
pause


