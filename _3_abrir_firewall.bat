@echo off
REM ===========================================================================
REM  (EXTRA) Forzar apertura de Windows Firewall para el puerto 8000 TCP.
REM  Solo hace falta ejecutarlo 1 sola vez. El _1_ ya lo incluye; esto es por si lo quieres separado.
REM ===========================================================================
chcp 65001 >nul
title SIFFA - Abriendo firewall puerto 8000
netsh advfirewall firewall delete rule name="SIFFA 8000" >nul 2>nul
netsh advfirewall firewall add rule name="SIFFA 8000" dir=in action=allow protocol=TCP localport=8000 profile=private,domain,public enable=yes
echo.
echo Regla creada. Otros equipos LAN ya pueden entrar al puerto 8000.
pause
