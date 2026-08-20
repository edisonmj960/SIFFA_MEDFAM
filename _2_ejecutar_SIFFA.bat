@echo off
REM ===========================================================================
REM  PASO 2 - Ejecutar SIFFA en modo LAN (todos los equipos de red pueden entrar)
REM  Doble clic en este archivo. Usamos WAITRESS (no Flask dev) por rendimiento.
REM ===========================================================================
chcp 65001 >nul
setlocal enableextensions enabledelayedexpansion
title SIFFA MedFam - Servidor LAN (http://0.0.0.0:8000)
cd /d "%~dp0"

if not exist ".venv\Scripts\waitress-serve.exe" (
    echo.
    echo [ERROR] Falta instalar. Primero ejecuta:  _1_instalar_requisitos.bat
    echo.
    pause
    exit /b 1
)

REM --- Detectar IP LAN (IPv4 privada; no 169.254) ---
set "MI_IP=127.0.0.1"
for /f "delims=[] tokens=2" %%a in ('ping -4 -n 1 %computername% ^| findstr "["') do (
    set "MI_IP=%%a"
)
REM Fallback por si el ping no da IP
if "%MI_IP%"=="127.0.0.1" (
    for /f "tokens=3" %%i in ('route print ^| findstr /r /c:"0.0.0.0.*0.0.0.0"') do (
        for /f "tokens=2 delims=:" %%d in ('ipconfig ^| findstr "IPv4"') do (
            for /f "tokens=*" %%e in ("%%d") do set "MI_IP=%%e"
        )
    )
)

cls
echo.
echo ======================================================================================
echo   SIFFA MedFam - SERVER ARRANCADO EN MODO RED LOCAL (LAN)
echo ======================================================================================
echo.
echo   Acceso desde ESTA MAQUINA:   http://localhost:8000/
echo   Acceso desde ESTA MAQUINA:   http://127.0.0.1:8000/
echo   Acceso OTROS EQUIPOS EN RED: http://%MI_IP%:8000/          ^^^^^ COMPARTE ESTA URL ^^^^^
echo.
echo   Diagnostico:                 http://%MI_IP%:8000/health
echo.
echo   Para DETENER: presiona CTRL + C en esta ventana (o cierra la ventana).
echo ======================================================================================
echo.
echo.
echo   Si hay ERROR de CONEXION con SISPRO (ConnectTimeout):
echo   - Asegurate que ESTA PC tenga IP COLOMBIANA whitelisteada ante SISPRO.
echo   - O edita el archivo _variables_entorno.bat y agrega HTTPS_PROXY, luego reinicia esto.
echo.

REM Cargar variables de entorno personalizadas si existen
if exist "_variables_entorno.bat" call "_variables_entorno.bat"

REM Si existe .env en raiz (compatibilidad dotenv) se usan; Windows no lee .env por defecto,
REM lo hacemos via PYTHONPATH + script.
set PYTHONPATH=%cd%;%PYTHONPATH%

REM Puerto y host fijos para red local
set WEB_HOST=0.0.0.0
set PORT=8000

REM --- Variables de red SIIFA por defecto (si el usuario no las seteo) ---
if "%SIIFA_SECURITY_BASEURL%"=="" set "SIIFA_SECURITY_BASEURL=https://siifa.sispro.gov.co/siifa-seguridad"
if "%SIIFA_FACTURA_BASEURL%"==""  set "SIIFA_FACTURA_BASEURL=https://siifa.sispro.gov.co/siifa-factura"
if "%SIIFA_MAX_RETRIES%"==""       set "SIIFA_MAX_RETRIES=3"
if "%SIIFA_CONNECT_TIMEOUT%"==""   set "SIIFA_CONNECT_TIMEOUT=10"
if "%SIIFA_READ_TIMEOUT%"==""      set "SIIFA_READ_TIMEOUT=60"
if "%SIIFA_SSL_VERIFY%"==""        set "SIIFA_SSL_VERIFY=1"

REM Lanzar waitress-serve (servidor WSGI producción de Python)
".venv\Scripts\waitress-serve.exe" --host=0.0.0.0 --port=8000 --threads=32 --connection-limit=64 --backlog=256 web_app:app

REM Si waitress se cierra
echo.
echo Servidor detenido.
pause
endlocal
