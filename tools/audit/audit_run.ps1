# Script de auditoria para monitorear la ejecucion de run.bat.
# Registra timestamps, procesos activos y exit codes.

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Resolve-Path (Join-Path $ScriptDir "..\..")
$AuditDir = Join-Path $ProjectRoot "logs\audit"
$AuditLog = Join-Path $AuditDir "audit_run_$(Get-Date -Format 'yyyyMMdd_HHmmss').log"
$RunOutputLog = Join-Path $AuditDir "run_output.log"
$RunErrorLog = Join-Path $AuditDir "run_error.log"
$StartTime = Get-Date

New-Item -ItemType Directory -Force -Path $AuditDir | Out-Null

function Log-Event {
    param([string]$Message)
    $Timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss.fff"
    $LogEntry = "[$Timestamp] $Message"
    Write-Host $LogEntry
    Add-Content -Path $AuditLog -Value $LogEntry
}

Log-Event "=== AUDIT RUN.BAT INICIADO ==="
Log-Event "Directorio del proyecto: $ProjectRoot"
Log-Event "PID actual: $PID"

# Iniciar run.bat y capturar salida en tiempo real.
Log-Event "Iniciando run.bat..."
$Process = Start-Process -FilePath "C:\Windows\System32\cmd.exe" `
    -ArgumentList "/c", "cd /d `"$ProjectRoot`" && .\run.bat all" `
    -NoNewWindow `
    -PassThru `
    -RedirectStandardOutput $RunOutputLog `
    -RedirectStandardError $RunErrorLog

Log-Event "PID del proceso: $($Process.Id)"

# Monitorear el proceso cada 5 segundos.
while (!$Process.HasExited) {
    Start-Sleep -Seconds 5
    $Elapsed = (Get-Date) - $StartTime

    $PythonProcs = Get-Process python -ErrorAction SilentlyContinue

    if ($PythonProcs) {
        Log-Event "Python activo: $($PythonProcs.Count) procesos | CPU: $($PythonProcs.CPU | Measure-Object -Sum | Select-Object -ExpandProperty Sum)%"
    } else {
        Log-Event "Sin procesos Python activos"
    }

    $FotoCarneLogs = Get-ChildItem -Path (Join-Path $ProjectRoot "logs\foto_carne") -Directory -ErrorAction SilentlyContinue | Sort-Object CreationTime -Descending | Select-Object -First 1
    $DjFutLogs = Get-ChildItem -Path (Join-Path $ProjectRoot "logs\dj_fut") -Directory -ErrorAction SilentlyContinue | Sort-Object CreationTime -Descending | Select-Object -First 1

    if ($FotoCarneLogs) {
        $FotoCarneLog = Get-Content "$($FotoCarneLogs.FullName)\foto_carne.log" -ErrorAction SilentlyContinue | Select-Object -Last 1
        if ($FotoCarneLog) {
            Log-Event "FOTO CARNE: $FotoCarneLog"
        }
    }

    if ($DjFutLogs) {
        $DjFutLog = Get-Content "$($DjFutLogs.FullName)\dj_fut.log" -ErrorAction SilentlyContinue | Select-Object -Last 1
        if ($DjFutLog) {
            Log-Event "DJ FUT: $DjFutLog"
        }
    }

    Log-Event "Tiempo transcurrido: $([int]$Elapsed.TotalSeconds) segundos"
}

Log-Event "Proceso terminado con codigo: $($Process.ExitCode)"
Log-Event "Tiempo total: $(Get-Date) - $StartTime"

# Mostrar ultimo contenido de los logs.
Log-Event "=== ULTIMAS LINEAS DE run_output.log ==="
$LastLine = Get-Content $RunOutputLog -Tail 5 -ErrorAction SilentlyContinue
Log-Event $LastLine

Log-Event "=== ULTIMAS LINEAS DE run_error.log ==="
$ErrorLine = Get-Content $RunErrorLog -Tail 5 -ErrorAction SilentlyContinue
Log-Event $ErrorLine

Log-Event "=== AUDIT COMPLETADO ==="
Write-Host "`nAudit guardado en: $AuditLog"
