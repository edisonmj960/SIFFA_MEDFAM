@echo off
REM ===========================================================================
REM  PASO 2 - Ejecutar SIFFA en modo LAN (todos los equipos de red pueden entrar)
REM  Doble clic en este archivo. Usamos WAITRESS (no Flask dev) por rendimiento.
REM
REM  NUNCA se cierra solo: al final hay un pause FORZADO.
REM  Todo lo que pasa queda LOGUEADO en _2_ejecutar_SIFFA.log
REM ===========================================================================
chcp 65001 >nul
setlocal enableextensions disabledelayedexpansion

title SIFFA MedFam - Servidor LAN (http://0.0.0.0:8000)

set "LOG_FILE=%~dpn0.log"
echo. >>"%LOG_FILE%" 2>nul
echo ======================================================== >>"%LOG_FILE%" 2>nul
echo   [%date% %time%] INICIANDO SIFFA >>"%LOG_FILE%" 2>nul
echo ======================================================== >>"%LOG_FILE%" 2>nul

call :LOG "Inicio"
cd /d "%~dp0"
if errorlevel 1 call :ERROR_EXIT "No se pudo cambiar a carpeta del script %~dp0"

REM --- 1. Chequear .venv y waitress ---
if not exist ".venv\Scripts\waitress-serve.exe" (
    call :LOG "[ERROR] Falta instalar. Primero ejecuta:  _1_instalar_requisitos.bat"
    call :LOG "         .venv\Scripts\waitress-serve.exe NO EXISTE."
    echo.
    echo [ERROR] Falta instalar. Primero ejecuta:  _1_instalar_requisitos.bat
    echo.
    goto :END_PAUSE
)

REM --- 2. Chequear web_app.py exista ---
if not exist "web_app.py" (
    call :LOG "[ERROR] No encuentro web_app.py en %cd%"
    echo.
    echo [ERROR] No encuentro web_app.py en %cd%
    goto :END_PAUSE
)

REM --- 3. Detectar IP LAN (IPv4 privada; no 169.254) ---
set "MI_IP=127.0.0.1"
for /f "usebackq delims=" %%a in (`powershell -NoProfile -Command ^
    "$ips = Get-NetIPAddress -AddressFamily IPv4 ^| Where-Object { $_.IPAddress -notlike '127.*' -and $_.IPAddress -notlike '169.254.*' -and ($_.PrefixOrigin -eq 'Dhcp' -or $_.PrefixOrigin -eq 'Manual') }; $i=$ips[0].IPAddress; if (-not $i) { $i=(Get-NetIPAddress -AddressFamily IPv4 ^| Where-Object { $_.IPAddress -notlike '127.*' -and $_.IPAddress -notlike '169.254.*' } ^| Select-Object -First 1 -ExpandProperty IPAddress) }; if (-not $i) { $i='127.0.0.1' }; Write-Output $i"`) do (
    set "MI_IP=%%a"
)
if "%MI_IP%"=="" set "MI_IP=127.0.0.1"
for /f "delims=[] tokens=2" %%a in ('ping -4 -n 1 %computername% 2^>nul ^| findstr "["') do (
    if "%MI_IP%"=="127.0.0.1" set "MI_IP=%%a"
)
call :LOG "IP detectada MI_IP=%MI_IP%"

REM --- 4. Banner pantalla y logs ---
cls
echo.
echo ======================================================================================
echo   SIFFA MedFam - SERVER ARRANCADO EN MODO RED LOCAL (LAN)
echo ======================================================================================
echo.
echo   Acceso desde ESTA MAQUINA:   http://localhost:8000/
echo   Acceso desde ESTA MAQUINA:   http://127.0.0.1:8000/
echo   Acceso OTROS EQUIPOS EN RED: http://%MI_IP%:8000/          ^^^ COMPARTE ESTA URL ^^^
echo.
echo   Diagnostico:                 http://%MI_IP%:8000/health
echo.
echo   Para DETENER: presiona CTRL + C en esta ventana (o cierra la ventana).
echo ======================================================================================
echo.
echo   Si hay ERROR de CONEXION con SISPRO (ConnectTimeout):
echo   - Asegurate que ESTA PC tenga IP COLOMBIANA whitelisteada ante SISPRO.
echo   - O renombra _-variables_entorno.bat a variables_entorno.bat y agrega HTTPS_PROXY
echo     luego reinicia este script.
echo.
echo   (Todo lo que pasa se guarda en: %LOG_FILE%)
echo.

REM --- 5. Cargar variables de entorno personalizadas ---
if exist "variables_entorno.bat" (
    call :LOG "Cargando variables_entorno.bat ..."
    call "variables_entorno.bat"
)
if exist "_variables_entorno.bat" (
    call :LOG "Cargando _variables_entorno.bat (legacy)..."
    call "_variables_entorno.bat"
)

REM --- 6. PYTHONPATH + puertos ---
set "PYTHONPATH=%cd%;%PYTHONPATH%"
set "WEB_HOST=0.0.0.0"
set "PORT=8000"

REM --- 7. Variables SIIFA defaults solo si NO estaban vacías ---
if "%SIIFA_SECURITY_BASEURL%"=="" set "SIIFA_SECURITY_BASEURL=https://siifa.sispro.gov.co/siifa-seguridad"
if "%SIIFA_FACTURA_BASEURL%"==""  set "SIIFA_FACTURA_BASEURL=https://siifa.sispro.gov.co/siifa-factura"
if "%SIIFA_MAX_RETRIES%"==""       set "SIIFA_MAX_RETRIES=3"
if "%SIIFA_CONNECT_TIMEOUT%"==""   set "SIIFA_CONNECT_TIMEOUT=10"
if "%SIIFA_READ_TIMEOUT%"==""      set "SIIFA_READ_TIMEOUT=60"
if "%SIIFA_SSL_VERIFY%"==""        set "SIIFA_SSL_VERIFY=1"

call :LOG "HTTP_PROXY=%HTTP_PROXY%"
call :LOG "HTTPS_PROXY=%HTTPS_PROXY%"
call :LOG "WEB_HOST=%WEB_HOST%  PORT=%PORT%"

REM --- 8. Chequeo Python en .venv ---
if not exist ".venv\Scripts\python.exe" (
    call :LOG "[ERROR] .venv\Scripts\python.exe NO EXISTE. Re-ejecuta _1_instalar_requisitos.bat"
    echo.
    echo [ERROR] .venv danado; re-ejecuta _1_instalar_requisitos.bat
    goto :END_PAUSE
)

REM --- 9. Chequear que web_app:app sea IMPORTABLE (antes de lanzar waitress) ---
call :LOG "Chequeando import de web_app ..."
".venv\Scripts\python.exe" -c "import web_app; print('Flask app import OK, url_map total:', len(list(web_app.app.url_map.iter_rules())))" 1>>"%LOG_FILE%" 2>>&1
if errorlevel 1 (
    call :LOG "[ERROR] FALLO importar web_app en Python. Ver log."
    echo.
    echo [ERROR PYTHON] No se pudo cargar web_app. Ver log: %LOG_FILE%
    echo Abre ese archivo y mira las ultimas lineas.
    goto :END_PAUSE
)
call :LOG "Import web_app OK."

REM --- 10. Ejecutar waitress-serve (PROCESO PRINCIPAL; se queda aqui hasta CTRL+C) ---
call :LOG "Lanzando waitress-serve en 0.0.0.0:8000 ..."
".venv\Scripts\waitress-serve.exe" --host=0.0.0.0 --port=8000 --threads=32 --connection-limit=64 --backlog=256 web_app:app
set "EXITCODE=%ERRORLEVEL%"
call :LOG "waitress-serve FINALIZADO. Codigo salida=%EXITCODE%"

REM --- 11. Mensaje despues de CTRL+C o cierre inesperado ---
echo.
echo Servidor detenido (exit code %EXITCODE%).
call :LOG "Salida del servidor"

goto :END_PAUSE

REM ========================================================================
REM  FUNCIONES AUXILIARES
REM ========================================================================
:LOG
echo [%date% %time%] %* >>"%LOG_FILE%"
exit /b 0

:ERROR_EXIT
echo.
echo [ERROR FATAL] %*
call :LOG "[ERROR FATAL] %*"
goto :END_PAUSE

:END_PAUSE
echo.
echo ========================================================
echo   Script terminado. Presiona cualquier tecla para cerrar...
echo ========================================================
echo [%date% %time%] FIN >>"%LOG_FILE%" 2>nul
echo.
pause >nul
endlocal
exit /b 0
