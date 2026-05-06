import os
import pathlib

pathlib.Path(".streamlit").mkdir(exist_ok=True)

secrets = f"""[auth]
redirect_uri = "{os.environ['REDIRECT_URI']}"
cookie_secret = "{os.environ['COOKIE_SECRET']}"
client_id = "{os.environ['GOOGLE_CLIENT_ID']}"
client_secret = "{os.environ['GOOGLE_CLIENT_SECRET']}"
server_metadata_url = "https://accounts.google.com/.well-known/openid-configuration"

[coresignal]
api_key = "{os.environ['CORESIGNAL_API_KEY']}"
"""

pathlib.Path(".streamlit/secrets.toml").write_text(secrets)
print("secrets.toml written successfully")
