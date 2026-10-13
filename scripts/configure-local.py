"""Create development-only configuration; never overwrite operator settings."""
from pathlib import Path
import base64
import re
import secrets

root = Path(__file__).resolve().parent.parent
destination = root / ".env"
if destination.exists():
    raise SystemExit("Keeping existing .env. Fill missing settings locally before starting Compose.")
text = (root / ".env.example").read_text()
values = {
    "AIRFLOW_FERNET_KEY": base64.urlsafe_b64encode(secrets.token_bytes(32)).decode(),
    "AIRFLOW_WEBSERVER_SECRET_KEY": secrets.token_hex(32),
}
for name, value in values.items():
    text = re.sub(r"^" + name + r"=.*$", name + "=" + value, text, flags=re.M)
with destination.open("x") as stream:
    destination.chmod(0o600)
    stream.write(text)
print("Created ignored .env with fresh local Airflow keys; replace database demo passwords before deployment.")
