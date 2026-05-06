#!/bin/bash
mkdir -p .streamlit
cat > .streamlit/secrets.toml << EOF
[auth]
redirect_uri = "${REDIRECT_URI}"
cookie_secret = "${COOKIE_SECRET}"
client_id = "${GOOGLE_CLIENT_ID}"
client_secret = "${GOOGLE_CLIENT_SECRET}"
server_metadata_url = "https://accounts.google.com/.well-known/openid-configuration"

[coresignal]
api_key = "${CORESIGNAL_API_KEY}"
EOF
streamlit run app.py --server.port $PORT --server.address 0.0.0.0
