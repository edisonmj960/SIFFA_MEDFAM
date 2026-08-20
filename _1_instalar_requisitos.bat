@echo off
REM ===========================================================================
REM  PASO 1 (solo UNA vez) — Instalar requisitos Python + entorno virtual
REM  Doble clic en este archivo.
REM ===========================================================================
chcp 65001 >nul
setlocal
title SIFFA - Instalando requisitos (1ra vez)
cd /d "%~dp0"

echo.
echo ========================================================
echo  SIFFA MedFam - Instalador de requisitos (1ra ejecucion)
echo ========================================================
echo.

REM ---- 1. Python disponible? ----
where python >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Python NO encontrado. Instala Python 3.9+ desde https://www.python.org/downloads/
    echo         Y MARCA "Add Python to PATH" durante la instalacion.
    echo.
    pause
    exit /b 1
)

echo [1/4] Python detectado:
python --version
echo.

REM ---- 2. Crear venv si no existe ----
if not exist ".venv\Scripts\python.exe" (
    echo [2/4] Creando entorno virtual .venv ...
    python -m venv .venv
    if %ERRORLEVEL% NEQ 0 (
        echo [ERROR] No se pudo crear .venv
        pause
        exit /b 2
    )
    echo       Entorno virtual creado OK.
) else (
    echo [2/4] Entorno virtual ya existe - saltando creacion.
)

REM ---- 3. Actualizar pip + instalar requirements.txt ----
echo.
echo [3/4] Instalando/Actualizando dependencias (requirements.txt + waitress)...
call ".venv\Scripts\activate.bat"
python -m pip install --upgrade pip setuptools wheel
pip install -r requirements.txt waitress
if %ERRORLEVEL% NEQ 0 (
    echo.
    echo [ERROR] Fallo al instalar dependencias. Revisa arriba.
    pause
    exit /b 3
)

REM ---- 4. Abrir puerto firewall ----
echo.
echo [4/4] Abriendo Windows Firewall para el puerto 8000 (LAN)...
netsh advfirewall firewall show rule name="SIFFA 8000" >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    netsh advfirewall firewall add rule name="SIFFA 8000" dir=in action=allow protocol=TCP localport=8000 profile=private,domain,public enable=yes >nul
    echo       Regla firewall creada.
) else (
    echo       Regla firewall ya existia - OK.
)

echo.
echo ========================================================
echo  INSTALACION FINALIZADA CORRECTAMENTE
echo ========================================================
echo.
echo  Ahora ejecuta:  _2_ejecutar_SIFFA.bat   (doble clic)
echo.
pause
endlocal
