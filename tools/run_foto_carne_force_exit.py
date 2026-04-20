import os
import re
import signal
import subprocess
import sys
import time
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
SCRIPT = ROOT_DIR / "run_foto_carne.py"
LOGS_ROOT = ROOT_DIR / "logs" / "foto_carne"

RUN_TIMEOUT_SECS = int(os.getenv("FOTO_CARNE_PROCESS_TIMEOUT_SECS", "900"))
EXIT_GRACE_SECS = int(os.getenv("FOTO_CARNE_EXIT_GRACE_SECS", "10"))
KILL_GRACE_SECS = int(os.getenv("FOTO_CARNE_KILL_GRACE_SECS", "8"))


def _pid_exists(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        result = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            check=False,
        )
        output = result.stdout or ""
        return str(pid) in output and "No se encuentran tareas" not in output and "No tasks are running" not in output

    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _terminate_process_win32(pid: int) -> None:
    if os.name != "nt":
        return
    try:
        import ctypes

        kernel32 = ctypes.windll.kernel32
        process_terminate = 0x0001
        handle = kernel32.OpenProcess(process_terminate, False, int(pid))
        if handle:
            try:
                kernel32.TerminateProcess(handle, 1)
            finally:
                kernel32.CloseHandle(handle)
    except Exception:
        pass


def _kill_process_tree(pid: int) -> None:
    if os.name == "nt":
        subprocess.run(
            ["taskkill", "/PID", str(pid), "/T", "/F"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        deadline = time.monotonic() + max(1, KILL_GRACE_SECS)
        while time.monotonic() < deadline:
            if not _pid_exists(pid):
                return
            time.sleep(0.5)
        _terminate_process_win32(pid)
        return

    try:
        os.killpg(pid, signal.SIGKILL)
    except Exception:
        try:
            os.kill(pid, signal.SIGKILL)
        except Exception:
            pass


def _start_child() -> subprocess.Popen:
    cmd = [sys.executable, "-u", str(SCRIPT)]
    kwargs = {
        "cwd": str(ROOT_DIR),
        "stdin": subprocess.DEVNULL,
    }
    if os.name == "nt":
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        kwargs["start_new_session"] = True
    return subprocess.Popen(cmd, **kwargs)


def _latest_foto_log(start_ts: float) -> Path | None:
    if not LOGS_ROOT.exists():
        return None

    candidates = []
    for path in LOGS_ROOT.iterdir():
        if not path.is_dir():
            continue
        try:
            if path.stat().st_mtime >= start_ts - 2:
                log_path = path / "foto_carne.log"
                if log_path.exists():
                    candidates.append(log_path)
        except OSError:
            continue

    if not candidates:
        return None
    return max(candidates, key=lambda item: item.stat().st_mtime)


def _completion_exit_code(log_path: Path | None) -> int | None:
    if log_path is None:
        return None
    try:
        lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return None

    for line in reversed(lines[-80:]):
        if "[FOTO CARNE] Flujo completado" not in line:
            continue
        match = re.search(r"errores=(\d+)", line)
        if match and int(match.group(1)) == 0:
            return 0
        return 1
    return None


def main() -> int:
    if not SCRIPT.exists():
        print(f"[FOTO CARNE WATCHDOG] No existe {SCRIPT}", file=sys.stderr)
        return 1

    start_ts = time.time()
    child = _start_child()
    print(f"[FOTO CARNE WATCHDOG] Proceso hijo iniciado. PID={child.pid}", flush=True)
    deadline = time.monotonic() + max(30, RUN_TIMEOUT_SECS)
    completed_at = None
    completed_code = None

    while True:
        exit_code = child.poll()
        if exit_code is not None:
            print(f"[FOTO CARNE WATCHDOG] Proceso hijo finalizo. Codigo={exit_code}", flush=True)
            return int(exit_code)

        if completed_code is None:
            completed_code = _completion_exit_code(_latest_foto_log(start_ts))
            if completed_code is not None:
                completed_at = time.monotonic()
                print(
                    f"[FOTO CARNE WATCHDOG] Log indica flujo completado. Codigo estimado={completed_code}",
                    flush=True,
                )

        if completed_at is not None and time.monotonic() - completed_at >= max(1, EXIT_GRACE_SECS):
            print("[FOTO CARNE WATCHDOG] Proceso hijo sigue vivo tras completar; forzando cierre.", flush=True)
            _kill_process_tree(child.pid)
            return int(completed_code)

        if time.monotonic() >= deadline:
            print(
                f"[FOTO CARNE WATCHDOG] Timeout de {RUN_TIMEOUT_SECS}s; forzando cierre del arbol Python.",
                file=sys.stderr,
                flush=True,
            )
            _kill_process_tree(child.pid)
            return 124

        time.sleep(1)


if __name__ == "__main__":
    sys.exit(main())
