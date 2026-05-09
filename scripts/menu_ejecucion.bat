@echo off
title POSE ETL - Menu de Ejecucion
color 0B

:: Determinar el directorio raiz del proyecto de forma dinamica
set "SCRIPT_DIR=%~dp0"
pushd "%SCRIPT_DIR%.."
set "PROJECT_ROOT=%CD%"
popd

:: Variables de entorno y rutas dinamicas
set "PYTHON=%PROJECT_ROOT%\.venv\Scripts\python.exe"
set "CONFIG_DIR=%PROJECT_ROOT%\config"

:: Verificacion del entorno virtual
if exist "%PYTHON%" (
    echo [OK] Entorno virtual encontrado.
) else (
    echo [ERROR] No se encontro el entorno virtual en:
    echo %PYTHON%
    echo Por favor, ejecuta primero 'python -m venv .venv' e instala los requirements.
    pause
    exit
)

:: Fijar directorio de trabajo a la raiz del proyecto
cd /d "%PROJECT_ROOT%"

:MENU
cls
echo =================================================
echo   POSE ETL - v1.0
echo =================================================
echo  FASE 1: PRE-INGESTA (Normalizacion Python)
echo  1. Normalizar + Alinear TODO
echo  2. Normalizar + Alinear SEGMENTO ESPECIFICO
echo  3. Solo Normalizacion
echo  4. Solo Alineacion para PQ
echo -------------------------------------------------
echo  FASE 2 y 3: POWER QUERY + REPORTES
echo  5. Actualizar Modelos Excel (Paso2)
echo  6. ETL COMPLETO (Fase 1 + Fase 2 + Fase 3)
echo -------------------------------------------------
echo  FASE 4: BIFURCADOR B52 (CSV para Hetzner/PostgreSQL)
echo  7. Bifurcador B52 - Completo (genera CSV + delta)
echo  8. Bifurcador B52 - Dry-run (solo resumen)
echo -------------------------------------------------
echo  CONFIGURACION
echo  9. Editar config_normalizador.json
echo 10. Editar config_automatizacion.json
echo  0. Salir
echo =================================================
set /p opcion="Selecciona una opcion: "

if "%opcion%"=="1"  goto RUN_ALL
if "%opcion%"=="2"  goto RUN_SEGMENT
if "%opcion%"=="3"  goto RUN_NORM
if "%opcion%"=="4"  goto RUN_ALINEAR
if "%opcion%"=="5"  goto RUN_PASO2
if "%opcion%"=="6"  goto RUN_ETL_COMPLETO
if "%opcion%"=="7"  goto RUN_BIFURCADOR
if "%opcion%"=="8"  goto RUN_BIFURCADOR_DRY
if "%opcion%"=="9"  goto EDIT_CONFIG
if "%opcion%"=="10" goto EDIT_CONFIG_AUTO
if "%opcion%"=="0"  exit
goto MENU

:RUN_ALL
echo. & echo [^>] Normalizando todos los segmentos...
"%PYTHON%" -m src.ingesta.normalizador_base_costos
echo. & echo [^>] Alineando para PQ...
"%PYTHON%" -m src.ingesta.alinear_para_ingesta
pause
goto MENU

:RUN_SEGMENT
set "SEGMENTO="
set "seg_op="

echo.
echo === SELECCIONA EL SEGMENTO ===
echo 1. 2021_2022_Historico
echo 2. Modificaciones
echo 3. 2025_Compensaciones
echo 4. 2023_2025_Hist
echo 5. 2025_Ajustes
echo 6. 2025
echo 7. 2026
set /p seg_op="Numero del segmento: "

if "%seg_op%"=="1" set "SEGMENTO=2021_2022_Historico"
if "%seg_op%"=="2" set "SEGMENTO=Modificaciones"
if "%seg_op%"=="3" set "SEGMENTO=2025_Compensaciones"
if "%seg_op%"=="4" set "SEGMENTO=2023_2025_Hist"
if "%seg_op%"=="5" set "SEGMENTO=2025_Ajustes"
if "%seg_op%"=="6" set "SEGMENTO=2025"
if "%seg_op%"=="7" set "SEGMENTO=2026"

if NOT DEFINED SEGMENTO (
    echo. & echo [ERROR] Opcion invalida.
    pause
    goto MENU
)

echo. & echo [^>] Normalizando segmento: %SEGMENTO%
"%PYTHON%" -m src.ingesta.normalizador_base_costos --segmento %SEGMENTO%
echo. & echo [^>] Alineando segmento: %SEGMENTO%
"%PYTHON%" -m src.ingesta.alinear_para_ingesta --segmento %SEGMENTO%
echo. & echo Proceso finalizado.
pause
goto MENU

:RUN_NORM
echo. & echo [^>] Ejecutando solo Normalizador (todos los segmentos)...
"%PYTHON%" -m src.ingesta.normalizador_base_costos
pause
goto MENU

:RUN_ALINEAR
echo. & echo [^>] Ejecutando solo Alineacion para PQ (todos los segmentos)...
"%PYTHON%" -m src.ingesta.alinear_para_ingesta
pause
goto MENU

:RUN_PASO2
echo. & echo [^>] Ejecutando Paso2 (Fase 2 + Fase 3 - Power Query)...
"%PYTHON%" scripts\Paso2_ActualizarPQ.py
pause
goto MENU

:RUN_ETL_COMPLETO
echo. & echo [^>] ETL COMPLETO iniciado...
echo [FASE 1] Normalizacion...
"%PYTHON%" -m src.ingesta.normalizador_base_costos
echo [FASE 1] Alineacion para PQ...
"%PYTHON%" -m src.ingesta.alinear_para_ingesta
echo. & echo [FASE 2-3] Power Query y Reportes...
"%PYTHON%" scripts\Paso2_ActualizarPQ.py
pause
goto MENU

:RUN_BIFURCADOR
echo. & echo [^>] Ejecutando Bifurcador B52...
"%PYTHON%" -m ETL_BaseA2.src.bifurcador.bifurcador
pause
goto MENU

:RUN_BIFURCADOR_DRY
echo. & echo [^>] Bifurcador B52 - Dry-run (sin escritura)...
"%PYTHON%" -m ETL_BaseA2.src.bifurcador.bifurcador --dry-run
pause
goto MENU

:EDIT_CONFIG
echo. & echo [^>] Abriendo config_normalizador.json...
start notepad "%CONFIG_DIR%\config_normalizador.json"
goto MENU

:EDIT_CONFIG_AUTO
echo. & echo [^>] Abriendo config_automatizacion.json...
start notepad "%CONFIG_DIR%\config_automatizacion.json"
goto MENU
