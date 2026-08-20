@echo off
REM ===========================================================================
REM  (OPCIONAL) Plantilla de variables de entorno para Windows
REM  Renombra este archivo quitando el guion al inicio:  variables_entorno.bat
REM  y edita tus valores.  _2_ejecutar_SIFFA.bat lo llamara automaticamente.
REM ===========================================================================

REM ---- PROXY COLOMBIANO (si SISPRO requiere whitelist IP CO) ----
REM set "HTTP_PROXY=http://usuario:clave@10.10.10.10:8080"
REM set "HTTPS_PROXY=http://usuario:clave@10.10.10.10:8080"
REM set "NO_PROXY=.sispro.gov.co,localhost,127.0.0.1"

REM ---- Si hay inspeccion TLS corporativa descomenta: ----
REM set "SIIFA_SSL_VERIFY=0"

REM ---- Tiempos (ajusta si la red es muy lenta) ----
REM set "SIIFA_CONNECT_TIMEOUT=15"
REM set "SIIFA_READ_TIMEOUT=90"
REM set "SIIFA_MAX_RETRIES=2"

REM ---- Usuario / pass por defecto? No se recomienda; usa el login ----
