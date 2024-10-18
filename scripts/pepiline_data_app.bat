@echo off
setlocal enabledelayedexpansion

echo Iniciando el proceso de limpieza de datos...

rem Activar el entorno virtual
call C:\Users\Usuario\Desktop\Gobierno_Limpieza_data\venv\Scripts\activate.bat

rem Directorio de logs
set LOG_DIR=C:\Users\Usuario\Desktop\Gobierno_Limpieza_data\logs

rem Limpiar logs anteriores
if exist %LOG_DIR%\*.log (
    del %LOG_DIR%\*.log
    echo Logs anteriores eliminados.
)

rem Función para ejecutar scripts
:run_script
echo Ejecutando %~n1.py...
python %1 >> %LOG_DIR%\%~n1.log 2>&1
if ERRORLEVEL 1 (
    echo Error en %~n1.py. Verifique el log en %LOG_DIR%\%~n1.log. Fecha y hora: %date% %time%
    exit /b 1
)
echo %~n1.py ejecutado correctamente.

rem Lista de scripts
set SCRIPTS=(
    "C:\Users\Usuario\Desktop\Gobierno_Limpieza_data\scripts\extract_data.py"
    "C:\Users\Usuario\Desktop\Gobierno_Limpieza_data\scripts\limpiezalibro1.py"
    "C:\Users\Usuario\Desktop\Gobierno_Limpieza_data\scripts\terrazas_limpio2.py"
    "C:\Users\Usuario\Desktop\Gobierno_Limpieza_data\scripts\licencias_sin_duplicar3.py"
    "C:\Users\Usuario\Desktop\Gobierno_Limpieza_data\scripts\archivos_integrados4.py"
    "C:\Users\Usuario\Desktop\Gobierno_Limpieza_data\scripts\superficie_agregada5.py"
    "C:\Users\Usuario\Desktop\Gobierno_Limpieza_data\scripts\archivos_concatenado.py"
    "C:\Users\Usuario\Desktop\Gobierno_Limpieza_data\scripts\sql_carga_d.py"
)

rem Ejecutar cada script en la lista
for %%s in %SCRIPTS% do (
    call :run_script %%s
    echo Proceso de limpieza del archivo %%~nxs completado.  rem Mensaje en la consola
)

echo Limpieza completada. Enviando datos a SQL Server...

rem Aquí puedes agregar la llamada para enviar el SMS
call :send_sms "El proceso de limpieza de datos ha sido completado."

echo Proceso completado.
pause

exit /b

:send_sms
rem Ejemplo de llamada a un servicio de SMS (Twilio, Nexmo, etc.)
rem Aquí deberías reemplazar con el comando específico para tu servicio de SMS.
rem Por ejemplo, usando curl para Twilio:
curl -X POST https://api.twilio.com/2010-04-01/Accounts/ACCOUNT_SID/Messages.json ^
--data-urlencode "To=DESTINO" ^
--data-urlencode "From=ORIGEN" ^
--data-urlencode "Body=%~1" ^
-u ACCOUNT_SID:AUTH_TOKEN

echo Mensaje SMS enviado: %~1
exit /b
