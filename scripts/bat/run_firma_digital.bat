@echo off
setlocal
cd /d "%~dp0\..\.."
echo [FIRMA DIGITAL] Ejecutando run_firma_digital.py
python -u tools\run_firma_digital_force_exit.py
set EXIT_CODE=%ERRORLEVEL%
echo [FIRMA DIGITAL] Proceso Python finalizado. Codigo=%EXIT_CODE%
if not "%EXIT_CODE%"=="0" (
  echo.
  echo [ERROR] run_firma_digital.py termino con codigo %EXIT_CODE%.
)
endlocal & exit /b %EXIT_CODE%
