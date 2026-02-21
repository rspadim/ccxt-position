import signal
import subprocess
import sys
import time


def _spawn() -> list[subprocess.Popen]:
    procs: list[subprocess.Popen] = []
    procs.append(
        subprocess.Popen([sys.executable, "-m", "apps.api.dispatcher_server"])
    )
    procs.append(
        subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "apps.api.main:app",
                "--host",
                "0.0.0.0",
                "--port",
                "8000",
            ]
        )
    )
    return procs


def main() -> int:
    children = _spawn()
    stopping = False

    def _shutdown(_sig: int, _frame: object) -> None:
        nonlocal stopping
        if stopping:
            return
        stopping = True
        for p in children:
            if p.poll() is None:
                p.terminate()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    exit_code = 0
    try:
        while True:
            for p in children:
                code = p.poll()
                if code is not None:
                    exit_code = code if code != 0 else exit_code
                    _shutdown(signal.SIGTERM, object())
                    for other in children:
                        if other is not p and other.poll() is None:
                            other.wait(timeout=10)
                    return exit_code
            time.sleep(0.5)
    finally:
        for p in children:
            if p.poll() is None:
                p.kill()


if __name__ == "__main__":
    raise SystemExit(main())
