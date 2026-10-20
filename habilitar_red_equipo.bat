@echo off
:: Solicitar permisos de Administrador automáticamente si no los tiene
net session >nul 2>&1
if %errorLevel% neq 0 (
    echo Solicitando permisos de Administrador...
    powershell -Command "Start-Process '%~f0' -Verb RunAs"
    exit /b
)

echo ==========================================================
echo    HABILITANDO ACCESO DE RED PARA COMPANEROS (BIG DATA)
echo ==========================================================
echo.

echo 1. Creando puente de red (PortProxy) para PostgreSQL (5432)...
netsh interface portproxy add v4tov4 listenport=5432 listenaddress=0.0.0.0 connectport=5432 connectaddress=127.0.0.1

echo 2. Creando puente de red (PortProxy) para MinIO S3 API (9000)...
netsh interface portproxy add v4tov4 listenport=9000 listenaddress=0.0.0.0 connectport=9000 connectaddress=127.0.0.1

echo 3. Creando puente de red (PortProxy) para MinIO Console (9001)...
netsh interface portproxy add v4tov4 listenport=9001 listenaddress=0.0.0.0 connectport=9001 connectaddress=127.0.0.1

echo 4. Habilitando puertos en el Firewall de Windows...
netsh advfirewall firewall add rule name="BigData Postgres 5432" dir=in action=allow protocol=TCP localport=5432 profile=any >nul 2>&1
netsh advfirewall firewall add rule name="BigData MinIO 9000" dir=in action=allow protocol=TCP localport=9000 profile=any >nul 2>&1
netsh advfirewall firewall add rule name="BigData MinIO 9001" dir=in action=allow protocol=TCP localport=9001 profile=any >nul 2>&1

echo.
echo ==========================================================
echo    ESTADO ACTUAL DE LOS PUENTES DE RED:
echo ==========================================================
netsh interface portproxy show all

echo.
echo ==========================================================
echo [EXITO] Puertos 5432, 9000 y 9001 expuestos a la red local.
echo Tus companeros ya pueden conectarse a tu IP: 10.175.132.244
echo ==========================================================
echo.
pause
