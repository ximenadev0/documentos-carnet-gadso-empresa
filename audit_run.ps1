# Script de auditoría para monitorear la ejecución de run.bat
# Registra timestamps, procesos activos y exit codes

$AuditLog = "C:\Users\bnunez\Desktop\BOTS SUCAMEC\documentos-carnet-gadso\audit_run_$(Get-Date -Format 'yyyyMMdd_HHmmss').log"
$StartTime = Get-Date

function Log-Event {
    param([string]$Message)
    $Timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss.fff"
    $LogEntry = "[$Timestamp] $Message"
    Write-Host $LogEntry
    Add-Content -Path $AuditLog -Value $LogEntry
}

Log-Event "=== AUDIT RUN.BAT INICIADO ==="
Log-Event "Directorio de trabajo: $(Get-Location)"
Log-Event "PID actual: $PID"

# Iniciar run.bat y capturar salida en tiempo real
Log-Event "Iniciando run.bat..."
$Process = Start-Process -FilePath "C:\Windows\System32\cmd.exe" `
    -ArgumentList "/c", "cd /d `"C:\Users\bnunez\Desktop\BOTS SUCAMEC\documentos-carnet-gadso`" && .\run.bat all" `
    -NoNewWindow `
    -PassThru `
    -RedirectStandardOutput "$(Split-Path $AuditLog -Parent)\run_output.log" `
    -RedirectStandardError "$(Split-Path $AuditLog -Parent)\run_error.log"

Log-Event "PID del proceso: $($Process.Id)"

# Monitorear el proceso cada 5 segundos
$LastLogCheck = 0
while (!$Process.HasExited) {
    Start-Sleep -Seconds 5
    $Elapsed = (Get-Date) - $StartTime
    
    # Obtener procesos python activos
    $PythonProcs = Get-Process python -ErrorAction SilentlyContinue
    
    if ($PythonProcs) {
        Log-Event "Python activo: $($PythonProcs.Count) procesos | CPU: $($PythonProcs.CPU | Measure-Object -Sum | Select-Object -ExpandProperty Sum)%"
    } else {
        Log-Event "Sin procesos Python activos"
    }
    
    # Verificar si hay nuevos logs
    $FotoCarneLogs = Get-ChildItem -Path "C:\Users\bnunez\Desktop\BOTS SUCAMEC\documentos-carnet-gadso\logs\foto_carne" -Directory -ErrorAction SilentlyContinue | Sort-Object CreationTime -Descending | Select-Object -First 1
    $DjFutLogs = Get-ChildItem -Path "C:\Users\bnunez\Desktop\BOTS SUCAMEC\documentos-carnet-gadso\logs\dj_fut" -Directory -ErrorAction SilentlyContinue | Sort-Object CreationTime -Descending | Select-Object -First 1
    
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

Log-Event "Proceso terminado con código: $($Process.ExitCode)"
Log-Event "Tiempo total: $(Get-Date) - $StartTime"

# Mostrar último contenido de los logs
Log-Event "=== ÚLTIMA LÍNEA DE run_output.log ==="
$LastLine = Get-Content "$(Split-Path $AuditLog -Parent)\run_output.log" -Tail 5
Log-Event $LastLine

Log-Event "=== ÚLTIMA LÍNEA DE run_error.log ==="
$ErrorLine = Get-Content "$(Split-Path $AuditLog -Parent)\run_error.log" -Tail 5
Log-Event $ErrorLine

Log-Event "=== AUDIT COMPLETADO ==="
Write-Host "`nAudit guardado en: $AuditLog"
