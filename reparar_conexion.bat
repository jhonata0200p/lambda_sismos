@echo off
net session >nul 2>&1
if %errorLevel% neq 0 (
    powershell -Command "Start-Process '%~f0' -Verb RunAs"
    exit /b
)

echo ==========================================================
echo    REPARANDO CONEXION A POSTGRESQL Y DOCKER
echo ==========================================================
echo.
echo Eliminando bucle de reenvio de puertos (PortProxy)...
netsh interface portproxy reset

echo.
echo ==========================================================
echo [EXITO] Puentes en bucle eliminados.
echo Ahora Docker recibira las conexiones directamente por tu IP.
echo ==========================================================
echo.
pause
