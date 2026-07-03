import json
from pathlib import Path

from google_auth_oauthlib.flow import InstalledAppFlow


SCOPES = ["https://www.googleapis.com/auth/drive"]


def main() -> None:
    client_path = Path(r"C:\Users\nicol\Downloads\gcp-oauth.keys.json")
    token_path = Path(r"C:\Users\nicol\Downloads\siga-gdrive-credentials.json")

    if not client_path.exists():
        raise SystemExit(f"OAuth client file not found: {client_path}")

    flow = InstalledAppFlow.from_client_secrets_file(str(client_path), SCOPES)
    creds = flow.run_local_server(
        host="127.0.0.1",
        port=0,
        authorization_prompt_message=(
            "Open this URL in your browser to authorize Hermes SIGA Drive access:\n{url}\n"
        ),
        success_message=(
            "Google Drive authorization received. You can close this browser tab."
        ),
        open_browser=True,
        access_type="offline",
        prompt="consent",
    )

    data = json.loads(creds.to_json())
    token_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    print(f"Wrote OAuth credentials to {token_path}")


if __name__ == "__main__":
    main()
