$ErrorActionPreference = "SilentlyContinue"

$Patterns = @(
    "tools\run_foto_carne_force_exit.py",
    "run_foto_carne.py",
    "tools\run_firma_digital_force_exit.py",
    "run_firma_digital.py"
)
$Processes = Get-CimInstance Win32_Process -Filter "name = 'python.exe'" |
    Where-Object {
        $CommandLine = $_.CommandLine
        $Patterns | Where-Object { $CommandLine -like "*$_*" }
    }

if (-not $Processes) {
    Write-Host "[BOT CLEANUP] No hay procesos python.exe de foto_carne/firma_digital."
    exit 0
}

$ParentIds = @()
foreach ($Process in $Processes) {
    Write-Host "[BOT CLEANUP] Cerrando python PID=$($Process.ProcessId) Parent=$($Process.ParentProcessId)"
    & taskkill /PID $Process.ProcessId /T /F | Out-Host
    $ParentIds += [int]$Process.ParentProcessId
}

Start-Sleep -Seconds 2

foreach ($ParentId in ($ParentIds | Sort-Object -Unique)) {
    $Parent = Get-CimInstance Win32_Process -Filter "ProcessId = $ParentId"
    if ($Parent -and $Parent.Name -ieq "cmd.exe") {
        Write-Host "[BOT CLEANUP] Cerrando cmd padre PID=$ParentId"
        & taskkill /PID $ParentId /T /F | Out-Host
    }
}

Start-Sleep -Seconds 1

$Remaining = Get-CimInstance Win32_Process -Filter "name = 'python.exe'" |
    Where-Object {
        $CommandLine = $_.CommandLine
        $Patterns | Where-Object { $CommandLine -like "*$_*" }
    }

if ($Remaining) {
    Write-Host "[BOT CLEANUP] Aun quedan procesos reportados por Windows:"
    $Remaining | Select-Object ProcessId, ParentProcessId, CommandLine | Format-List
    Write-Host "[BOT CLEANUP] Si taskkill dice que no hay instancia activa, cierre sesion o reinicie Windows para limpiar entradas zombis."
    exit 1
}

Write-Host "[BOT CLEANUP] Limpieza completada."
exit 0
