import os


def load_env_file(dotenv_path: str = "/home/pi/source_code/.env") -> None:
    """
    Minimal .env loader (KEY=VALUE lines, supports comments and blank lines).
    Does not override already-set environment variables.
    """
    if not dotenv_path or not os.path.exists(dotenv_path):
        return

    with open(dotenv_path, "r") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            if not key:
                continue
            os.environ.setdefault(key, value)


def require_env(*keys: str) -> None:
    missing = [k for k in keys if not os.getenv(k)]
    if missing:
        raise RuntimeError(
            "Missing required environment variables: "
            + ", ".join(missing)
            + ". Set them in /home/pi/source_code/.env or in the environment."
        )

